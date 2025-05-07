import json
import threading
import time
import os
import sys
import argparse
import configparser
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Callable, Optional

from loguru import logger
from confluent_kafka import Producer
from pymongo import MongoClient

# Import custom modules (Đảm bảo file kafka_metrics.py tồn tại và đúng cấu trúc)
# Nếu chưa có, bạn cần tạo file này hoặc comment out các phần liên quan đến metrics
try:
    from kafka_metrics import setup_metrics, get_metrics
    METRICS_ENABLED = True
except ImportError:
    logger.warning("kafka_metrics module not found. Metrics will be disabled.")
    # Tạo một đối tượng giả để tránh lỗi nếu module không tồn tại
    class MockMetrics:
        def __getattr__(self, name):
            # Trả về một hàm hoặc đối tượng giả không làm gì cả
            def mock_func(*args, **kwargs):
                if name.endswith('_time') or name == 'app_total_processing_time':
                    # Trả về một context manager giả
                    class MockTimer:
                        def __enter__(self): return self
                        def __exit__(self, *args): pass
                    return MockTimer()
                elif name.endswith('.inc') or name.endswith('.dec') or name.endswith('.set'):
                    return lambda *a, **k: None # Hàm không làm gì cả
                elif name.startswith('register_') or name.startswith('update_') or name.startswith('begin_') or name.startswith('end_') or name.startswith('track_'):
                     return lambda *a, **k: None # Hàm không làm gì cả
                elif name.endswith('.collect'):
                    # Trả về cấu trúc dữ liệu giả cho collect
                    class MockSample: value = 0
                    class MockMetricFamily: samples = [MockSample()]
                    return lambda: [MockMetricFamily()]
                return self # Trả về chính nó cho các trường hợp khác

            return mock_func

    class MockMetricsContainer:
        def __init__(self):
            # Thêm các thuộc tính metrics giả nếu cần thiết trong code
            self.mongodb_read_time = MockMetrics()
            self.mongodb_records_read = MockMetrics()
            self.mongodb_connection_errors = MockMetrics()
            self.mongodb_read_rate = MockMetrics()
            self.kafka_producer_active = MockMetrics()
            self.kafka_connection_errors = MockMetrics()
            self.kafka_records_failed = MockMetrics()
            self.kafka_records_sent = MockMetrics()
            self.kafka_batch_processing_time = MockMetrics()
            self.kafka_send_rate = MockMetrics()
            self.kafka_success_rate = MockMetrics()
            self.app_total_processing_time = MockMetrics()
            self.producer_send_rate = {} # Sẽ cần xử lý đặc biệt nếu dùng

        def register_producer(self, producer_id):
            # Khởi tạo metrics giả cho producer nếu cần
            self.producer_send_rate[producer_id] = MockMetrics()
            pass

        def update_producer_metrics(self, *args, **kwargs): pass
        def begin_batch_processing(self, *args, **kwargs): pass
        def end_batch_processing(self, *args, **kwargs): pass
        def track_memory_and_cpu(self, *args, **kwargs): pass
        def update_kafka_success_rate(self, *args, **kwargs): pass


    def setup_metrics(*args, **kwargs):
        logger.info("Metrics are disabled.")
        return MockMetricsContainer()

    METRICS_ENABLED = False

# Cấu hình loguru
logger.remove()  # Xóa cấu hình mặc định

os.makedirs("logs", exist_ok=True)

# Log ra console
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO"
)

# Log ra file trong thư mục logs
logger.add(
    "logs/mongo_to_kafka_{time}.log",
    rotation="100 MB",
    retention="7 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    level="DEBUG" # Đặt DEBUG để thấy log chi tiết hơn khi cần
)

# Mặc định
DEFAULT_CONFIG = {
    "mongodb": {
        "uri": "mongodb://localhost:27017",
        "db": "user_fcm",
        "collection": "users_fcm",
        "query": "{}",
        "projection": "{}",
        "limit": "0", # 0 nghĩa là không giới hạn (ngoại trừ giới hạn cứng 1000 trong code)
        "batch_size": "1000"
    },
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic": "test",
        "num_producers": "10",
        "batch_size": "5000", # queue.buffering.max.messages sẽ được dùng thay thế, batch.num.messages nên được đặt ở producer config
        "linger_ms": "5",
        "simulate_failure_rate": "0.1"  # <<< Tỷ lệ lỗi mô phỏng (0.1 = 10%)
    },
    "monitoring": {
        "prometheus_port": "8000",
        "track_interval_seconds": "5",
        "detailed_producer_metrics": "true" # Cấu hình này chưa được sử dụng, nhưng để tham khảo
    }
}

def generate_trace_id():
    """
    Tạo trace_id duy nhất cho mỗi bản ghi

    Returns:
        str: Trace ID duy nhất
    """
    return str(uuid.uuid4())

def load_config(config_path: Optional[str] = None) -> configparser.ConfigParser:
    """
    Tải cấu hình từ file (nếu có) hoặc dùng mặc định

    Args:
        config_path: Đường dẫn đến file cấu hình

    Returns:
        configparser.ConfigParser: Đối tượng cấu hình
    """
    config = configparser.ConfigParser()

    # Tải cấu hình mặc định
    for section, options in DEFAULT_CONFIG.items():
        if section not in config:
            config[section] = {}
        for key, value in options.items():
            # Chỉ đặt giá trị mặc định nếu key chưa tồn tại từ file
            if key not in config[section]:
                config[section][key] = value

    # Tải cấu hình từ file nếu có và ghi đè giá trị mặc định
    if config_path and os.path.exists(config_path):
        logger.info(f"Đang tải cấu hình từ file: {config_path}")
        config.read(config_path)
    else:
        logger.info("Không tìm thấy file cấu hình hoặc không được cung cấp. Sử dụng cấu hình mặc định.")
        # Nếu không có file config, cần đảm bảo các section mặc định được tạo
        for section, options in DEFAULT_CONFIG.items():
            if section not in config:
                config.add_section(section)
        for key, value in options.items():
                if key not in config[section]:
                    config.set(section, key, value)


    return config

def format_duration(seconds: float) -> str:
    """
    Định dạng thời gian từ giây sang chuỗi dễ đọc

    Args:
        seconds: Số giây

    Returns:
        str: Chuỗi thời gian đã định dạng
    """
    if seconds < 0: seconds = 0
    duration = timedelta(seconds=seconds)
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        # Hiển thị cả milliseconds nếu dưới 1 phút
        ms = int(duration.microseconds / 1000)
        return f"{seconds}s {ms}ms"

class MongoToKafkaMonitor:
    """
    Lớp chính để quản lý quá trình lấy dữ liệu từ MongoDB và gửi lên Kafka
    với theo dõi metrics và trace_id, cùng khả năng mô phỏng lỗi.
    """
    def __init__(self, config: configparser.ConfigParser):
        """
        Khởi tạo monitor

        Args:
            config: Cấu hình
        """
        self.config = config

        # Cấu hình MongoDB
        self.mongo_uri = config['mongodb']['uri']
        self.mongo_db = config['mongodb']['db']
        self.mongo_collection = config['mongodb']['collection']
        self.mongo_query = json.loads(config['mongodb']['query'])
        self.mongo_projection = json.loads(config['mongodb']['projection'])
        self.mongo_limit = int(config['mongodb']['limit']) # Giới hạn tổng thể (0 là không giới hạn)
        self.mongo_batch_size = int(config['mongodb']['batch_size'])
        # Thêm giới hạn cứng để tránh đọc quá nhiều dữ liệu vào bộ nhớ cùng lúc
        self.hard_limit = 1000000 # Ví dụ: Giới hạn cứng 1 triệu bản ghi đọc từ Mongo
        logger.info(f"Giới hạn cứng số bản ghi đọc từ MongoDB: {self.hard_limit:,}")


        # Cấu hình Kafka
        self.kafka_bootstrap_servers = config['kafka']['bootstrap_servers']
        self.kafka_topic = config['kafka']['topic']
        self.kafka_num_producers = int(config['kafka']['num_producers'])
        # self.kafka_batch_size = int(config['kafka']['batch_size']) # Không dùng trực tiếp, dùng producer config
        self.kafka_linger_ms = int(config['kafka']['linger_ms'])

        # Lấy tỷ lệ lỗi mô phỏng từ config, mặc định là 0 nếu không có
        try:
            # Sử dụng getfloat với fallback để an toàn hơn
            self.simulate_failure_rate = config.getfloat('kafka', 'simulate_failure_rate', fallback=0.0)
            if not (0.0 <= self.simulate_failure_rate <= 1.0):
                logger.warning(f"Giá trị simulate_failure_rate ({self.simulate_failure_rate}) không hợp lệ. Sử dụng 0.0. Phải nằm trong khoảng [0.0, 1.0].")
                self.simulate_failure_rate = 0.0
            elif self.simulate_failure_rate > 0:
                logger.warning(f"*** MÔ PHỎNG LỖI KAFKA ĐANG ĐƯỢC KÍCH HOẠT VỚI TỶ LỆ: {self.simulate_failure_rate * 100:.1f}% ***")
        except (KeyError, ValueError, configparser.NoOptionError, configparser.NoSectionError) as e:
            logger.info(f"Không tìm thấy hoặc giá trị simulate_failure_rate không hợp lệ trong config. Lỗi: {e}. Vô hiệu hóa mô phỏng lỗi.")
            self.simulate_failure_rate = 0.0

        # Cấu hình giám sát
        self.prometheus_port = int(config['monitoring']['prometheus_port'])
        self.track_interval = int(config['monitoring']['track_interval_seconds'])

        # Khởi tạo metrics (sử dụng setup_metrics đã import)
        self.metrics = setup_metrics(port=self.prometheus_port)
        if not METRICS_ENABLED:
            logger.warning("Metrics đang bị vô hiệu hóa do module kafka_metrics không được tìm thấy.")


        # Biến theo dõi
        self.start_time = None
        self.end_time = None
        self.producer_start_times = {} # Dùng dict để lưu theo producer_id
        self.producer_end_times = {}   # Dùng dict
        self.producer_records = {}     # Dùng dict: {producer_id: count}

        # Biến để theo dõi quá trình metrics
        self.monitor_thread = None
        self.running = False

        logger.info(f"Đã khởi tạo MongoToKafkaMonitor với tối đa {self.kafka_num_producers} producers")
        if self.simulate_failure_rate > 0:
            logger.warning(f"Mô phỏng lỗi Kafka đang bật với tỷ lệ {self.simulate_failure_rate*100:.1f}%")

    def load_from_mongodb(self) -> List[Dict[str, Any]]:
        """
        Tải dữ liệu từ MongoDB, giới hạn cố định 10,000 bản ghi.

        Returns:
            List[Dict[str, Any]]: Danh sách tối đa 10,000 bản ghi.
        """
        start_time = time.time()
        logger.info(f"Kết nối đến MongoDB: {self.mongo_uri}, DB: {self.mongo_db}, Collection: {self.mongo_collection}")

        try:
            with MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000) as client: # Thêm timeout
                client.admin.command('ping') # Kiểm tra kết nối
                logger.info("Kết nối MongoDB thành công.")
                db = client[self.mongo_db]
                collection = db[self.mongo_collection]

                # --- THAY ĐỔI LOGIC GIỚI HẠN ---
                # Bỏ qua việc đếm và các giới hạn khác, áp dụng giới hạn cố định 10,000
                limit_to_apply = 10000
                logger.info(f"Áp dụng giới hạn truy vấn cố định là {limit_to_apply:,} bản ghi.")
                # --- KẾT THÚC THAY ĐỔI ---

                data = []
                records_read_count = 0

                # Sử dụng metrics để theo dõi (nếu bật)
                with self.metrics.mongodb_read_time.time():
                    cursor = collection.find(
                        filter=self.mongo_query,
                        projection=self.mongo_projection
                    ).batch_size(self.mongo_batch_size)

                    # Áp dụng giới hạn cố định đã xác định
                    cursor = cursor.limit(limit_to_apply)

                    logger.info("Bắt đầu đọc dữ liệu từ MongoDB cursor (tối đa 10,000 bản ghi)...")
                    for record in cursor:
                        try:
                            if "_id" in record:
                                record["_id"] = str(record["_id"])

                            record["trace_id"] = generate_trace_id()
                            record["_mongodb_timestamp"] = datetime.now().isoformat()
                            record["_source_system"] = "mongodb"

                            data.append(record)
                            records_read_count += 1

                            # Cập nhật metrics
                            self.metrics.mongodb_records_read.inc()

                            # Log tiến độ đọc (có thể không cần thiết với giới hạn nhỏ)
                            if records_read_count % 2000 == 0: # Log mỗi 2k records
                                elapsed_so_far = time.time() - start_time
                                rate_so_far = records_read_count / elapsed_so_far if elapsed_so_far > 0 else 0
                                logger.info(f"Đã đọc {records_read_count:,}/{limit_to_apply:,} bản ghi... ({rate_so_far:.2f} recs/s)")

                        except Exception as record_err:
                            logger.error(f"Lỗi xử lý bản ghi MongoDB: {record_err}. Bản ghi: {record}")
                            # Có thể bỏ qua hoặc xử lý lỗi tùy theo yêu cầu

                elapsed_time = time.time() - start_time

                if elapsed_time > 0:
                    read_rate = records_read_count / elapsed_time
                    self.metrics.mongodb_read_rate.set(read_rate)
                    logger.success(f"Đã đọc xong {records_read_count:,} bản ghi (đáp ứng giới hạn {limit_to_apply:,}) từ MongoDB trong {format_duration(elapsed_time)} ({read_rate:.2f} bản ghi/giây)")
                else:
                    logger.success(f"Đã đọc xong {records_read_count:,} bản ghi (đáp ứng giới hạn {limit_to_apply:,}) từ MongoDB")

                # Đảm bảo rằng không trả về nhiều hơn giới hạn (mặc dù .limit() đã xử lý)
                return data[:limit_to_apply]

        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng khi kết nối hoặc đọc từ MongoDB: {str(e)}")
            self.metrics.mongodb_connection_errors.inc()
            return []

    def create_kafka_producer(self, producer_id: int) -> Optional[Producer]:
        """
        Tạo một Kafka producer

        Args:
            producer_id: ID của producer

        Returns:
            Producer: Đối tượng producer hoặc None nếu lỗi
        """
        try:
            conf = {
                "bootstrap.servers": self.kafka_bootstrap_servers,
                "client.id": f"producer-{producer_id}-{uuid.uuid4()}", # Thêm uuid để tránh trùng client.id khi chạy lại nhanh
                # --- Các cấu hình quan trọng cho hiệu năng ---
                # Kích thước tối đa của hàng đợi nội bộ (trước khi produce block hoặc lỗi)
                "queue.buffering.max.messages": 1000000, # Tăng lên để chứa nhiều message hơn
                # Kích thước tối đa của một batch gửi đi (bytes)
                "batch.size": 1024 * 1024, # 1MB batch size
                # Thời gian chờ tối đa để gom batch (ms)
                "linger.ms": self.kafka_linger_ms, # Sử dụng giá trị từ config
                # Thuật toán nén (lz4 thường cân bằng tốt giữa tốc độ và tỷ lệ nén)
                "compression.type": "lz4",
                # Số lần thử lại nếu gửi lỗi (quan trọng để xử lý lỗi tạm thời)
                "retries": 5,
                # Thời gian chờ giữa các lần thử lại (ms)
                "retry.backoff.ms": 100,
                # Cấu hình ACK: 'all' đảm bảo leader và ISRs nhận được, an toàn nhất nhưng chậm nhất
                # '1' (mặc định): chỉ leader nhận được, nhanh hơn nhưng có thể mất dữ liệu nếu leader fail
                # '0': không chờ ACK, nhanh nhất nhưng dễ mất dữ liệu nhất
                "acks": "1" # Cân bằng giữa tốc độ và độ bền
            }
            logger.debug(f"Cấu hình Kafka Producer {producer_id}: {conf}")
            producer = Producer(conf)

            self.metrics.kafka_producer_active.inc()
            self.metrics.register_producer(producer_id) # Đảm bảo hàm này tồn tại trong metrics

            logger.info(f"Đã tạo Kafka producer {producer_id} thành công.")
            return producer

        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng khi tạo Kafka producer {producer_id}: {str(e)}")
            self.metrics.kafka_connection_errors.inc()
            return None # Trả về None thay vì raise để task có thể xử lý

    def delivery_report(self, err, msg):
        """
        Callback được gọi khi message được gửi đi hoặc gặp lỗi.
        Cập nhật metrics tương ứng.

        Args:
            err: Lỗi nếu có (KafkaError)
            msg: Message đã gửi (Message)
        """
        trace_id = "unknown"
        record_id = "unknown"
        producer_id = "unknown"

        try:
            # Lấy thông tin từ headers đã được gửi đi
            if msg and msg.headers():
                headers_dict = {h[0]: h[1].decode('utf-8') for h in msg.headers() if h[1]}
                trace_id = headers_dict.get("trace_id", trace_id)
                record_id = headers_dict.get("record_id", record_id)
                producer_id = headers_dict.get("producer_id", producer_id)
        except Exception as e:
            # Lỗi này không nên xảy ra thường xuyên
            logger.warning(f"Không thể đọc headers từ Kafka callback message: {str(e)}")

        if err is not None:
            # Lỗi thực sự từ Kafka Broker hoặc network
            logger.error(f"KAFKA_DELIVERY [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=delivery_error] [error_code={err.code()}] [error_msg={err.str()}]")
            self.metrics.kafka_records_failed.inc() # <<< Cập nhật metric lỗi
        else:
            # Gửi THÀNH CÔNG và được broker ACK
            # Log ở mức INFO hoặc DEBUG tùy nhu cầu
            logger.debug(f"KAFKA_DELIVERY [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=delivered] [partition={msg.partition()}] [offset={msg.offset()}]")
            self.metrics.kafka_records_sent.inc() # <<< Cập nhật metric thành công

    def producer_task(self, data: List[Dict[str, Any]], producer_id: int) -> Dict[str, Any]:
        """
        Hàm được chạy trong từng thread để gửi dữ liệu, có mô phỏng lỗi.

        Args:
            data: Dữ liệu cần gửi cho producer này.
            producer_id: ID của producer.

        Returns:
            Dict[str, Any]: Kết quả gửi dữ liệu của producer này.
        """
        if not data:
            logger.warning(f"Producer {producer_id} nhận được danh sách dữ liệu rỗng.")
            return {
                "producer_id": producer_id,
                "attempted_success_count": 0,
                "task_failure_count": 0,
                "total_count": 0,
                "elapsed_time": 0,
                "rate": 0
            }

        self.producer_start_times[producer_id] = time.time()
        producer = self.create_kafka_producer(producer_id)

        # Nếu không tạo được producer, coi như tất cả đều lỗi
        if producer is None:
            logger.error(f"Producer {producer_id} không thể khởi tạo. Tất cả {len(data)} bản ghi được coi là lỗi.")
            self.metrics.kafka_records_failed.inc(len(data))
            end_time = time.time()
            elapsed = end_time - self.producer_start_times.get(producer_id, end_time)
            return {
                "producer_id": producer_id,
                "attempted_success_count": 0,
                "task_failure_count": len(data),
                "total_count": len(data),
                "elapsed_time": elapsed,
                "rate": 0,
                "error": "Producer initialization failed"
            }


        attempted_success_count = 0 # Số lần gọi producer.produce thành công (chưa chắc đã delivery)
        task_failure_count = 0      # Số lỗi trong task (gồm simulated + lỗi chuẩn bị/produce)
        total_records_in_task = len(data)

        try:
            logger.info(f"Producer {producer_id} bắt đầu xử lý {total_records_in_task:,} bản ghi")

            # Sử dụng context manager cho metrics thời gian xử lý batch của producer này
            with self.metrics.kafka_batch_processing_time.time():
                # Không cần begin/end batch processing ở đây vì metrics tổng thể đã có
                # self.metrics.begin_batch_processing(total_records_in_task)

                for idx, record in enumerate(data):
                    record_id = "unknown"
                    trace_id = "unknown"
                    try:
                        # Lấy thông tin định danh bản ghi
                        record_id = record.get("_id", "unknown")
                        trace_id = record.get("trace_id")
                        if not trace_id:
                            trace_id = generate_trace_id()
                            record["trace_id"] = trace_id # Gán lại vào record

                        # --- LOGIC MÔ PHỎNG LỖI ---
                        if self.simulate_failure_rate > 0 and random.random() < self.simulate_failure_rate:
                            error_msg = "Simulated Kafka Produce Failure"
                            current_time = datetime.now().isoformat()

                            # Ghi log lỗi mô phỏng
                            logger.error(f"KAFKA_EVENT [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=simulated_error] [error={error_msg}]")

                            # Cập nhật record với thông tin lỗi (tùy chọn)
                            record["status"] = "simulated_error"
                            record["error_message"] = error_msg
                            record["error_timestamp"] = current_time
                            record["_kafka_producer_id"] = producer_id
                            record["_processing_phase"] = "kafka_producer_simulation"

                            # Cập nhật metrics lỗi và bộ đếm lỗi của task
                            self.metrics.kafka_records_failed.inc()
                            task_failure_count += 1

                            # Quan trọng: Bỏ qua việc gửi message này
                            continue # Đi đến bản ghi tiếp theo
                        # --- KẾT THÚC LOGIC MÔ PHỎNG LỖI ---

                        # Nếu không mô phỏng lỗi, tiếp tục gửi như bình thường
                        current_time = datetime.now().isoformat()
                        record["_kafka_timestamp"] = current_time
                        record["_kafka_producer_id"] = producer_id
                        record["_processing_phase"] = "kafka_producer"
                        record["status"] = "sending_to_kafka" # Trạng thái trước khi gọi produce

                        # Log trước khi gửi (có thể là DEBUG)
                        logger.debug(f"KAFKA_EVENT [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=sending]")

                        record_json = json.dumps(record)

                        # Gọi producer.produce - đây là non-blocking
                        producer.produce(
                            topic=self.kafka_topic,
                            value=record_json.encode('utf-8'),
                            headers={
                                "trace_id": trace_id.encode('utf-8'),
                                "record_id": str(record_id).encode('utf-8'),
                                "producer_id": str(producer_id).encode('utf-8'),
                                "timestamp": current_time.encode('utf-8'),
                                "source": "mongodb_to_kafka".encode('utf-8')
                            },
                            # Callback sẽ xử lý kết quả (thành công/lỗi) delivery
                            callback=self.delivery_report
                        )

                        # Tăng bộ đếm số lần gọi produce thành công
                        attempted_success_count += 1

                        # Poll định kỳ để xử lý các delivery report callbacks
                        # Poll(0) là non-blocking, giúp xử lý callback nhanh chóng
                        producer.poll(0)

                        # Log sau khi gọi produce (có thể là DEBUG)
                        logger.debug(f"KAFKA_EVENT [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=produced]")

                        # Flush định kỳ để tránh đầy bộ đệm producer và kích hoạt gửi batch
                        # Điều chỉnh tần suất flush nếu cần
                        if (idx + 1) % 5000 == 0:
                            logger.debug(f"Producer {producer_id}: Flushing buffer at record {idx + 1}...")
                            producer.flush(timeout=5) # Timeout ngắn để không block quá lâu
                            logger.debug(f"Producer {producer_id}: Flush complete.")

                            # Log tiến độ và cập nhật rate (dựa trên số đã xử lý trong task)
                            elapsed = time.time() - self.producer_start_times[producer_id]
                            if elapsed > 0:
                                current_processed = idx + 1
                                rate = current_processed / elapsed
                                logger.info(f"Producer {producer_id}: Processed {current_processed:,}/{total_records_in_task:,} records (Current Rate: {rate:.2f} records/sec)")
                                # Cập nhật metric rate của producer này (nếu có)
                                if producer_id in self.metrics.producer_send_rate:
                                    self.metrics.producer_send_rate[producer_id].set(rate)


                    except Exception as e:
                        # Lỗi xảy ra *trong* vòng lặp khi chuẩn bị hoặc gọi produce
                        current_time = datetime.now().isoformat()
                        logger.error(f"KAFKA_EVENT [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=producer_error] [error={str(e)}]")
                        try:
                            record["status"] = "producer_error"
                            record["error_message"] = str(e)
                            record["error_timestamp"] = current_time
                        except: pass # Đề phòng record không phải dict

                        # Cập nhật metrics lỗi và bộ đếm lỗi của task
                        self.metrics.kafka_records_failed.inc()
                        task_failure_count += 1

            # --- Kết thúc vòng lặp ---
            logger.info(f"Producer {producer_id}: Đã xử lý xong vòng lặp {total_records_in_task:,} bản ghi.")

            # Flush lần cuối để đảm bảo mọi message trong buffer được gửi đi
            # và chờ các delivery report callbacks được xử lý
            logger.info(f"Producer {producer_id}: Thực hiện final flush và chờ delivery reports...")
            try:
                # Tăng timeout để cho phép xử lý các callback còn lại
                remaining = producer.flush(timeout=60) # Chờ tối đa 60 giây
                if remaining > 0:
                    # Nếu vẫn còn message sau khi flush timeout, có thể chúng đã bị lỗi hoặc mạng chậm
                    logger.warning(f"Producer {producer_id}: {remaining} messages có thể chưa được gửi hoặc xác nhận sau final flush timeout.")
                    # Quyết định xem có nên coi những message này là lỗi hay không
                    # self.metrics.kafka_records_failed.inc(remaining)
                    # task_failure_count += remaining
                else:
                    logger.info(f"Producer {producer_id}: Final flush hoàn tất, không còn message chờ.")
            except Exception as flush_err:
                # Lỗi có thể xảy ra nếu producer gặp vấn đề nghiêm trọng
                logger.error(f"Producer {producer_id}: Lỗi trong quá trình final flush: {flush_err}")

            # Tính toán thời gian và rate cuối cùng cho producer này
            self.producer_end_times[producer_id] = time.time()
            elapsed_time = self.producer_end_times[producer_id] - self.producer_start_times[producer_id]
            # Rate dựa trên tổng số bản ghi được giao cho task này
            rate = total_records_in_task / elapsed_time if elapsed_time > 0 else 0

            # Cập nhật metrics cuối cùng cho producer này (dùng số liệu từ task)
            # Lưu ý: Số liệu thành công/thất bại cuối cùng nên lấy từ metrics tổng thể sau khi tất cả producer hoàn thành
            self.metrics.update_producer_metrics(
                producer_id=producer_id,
                # Sử dụng attempted_success_count vì đây là số producer đã cố gửi
                records_sent=attempted_success_count,
                elapsed_time=elapsed_time
            )

            logger.success(f"Producer {producer_id} hoàn thành task: total={total_records_in_task:,}, attempted_produce={attempted_success_count:,}, task_failures={task_failure_count:,} "
                            f"trong {format_duration(elapsed_time)} ({rate:.2f} bản ghi/giây)")

            return {
                "producer_id": producer_id,
                "attempted_success_count": attempted_success_count, # Số lần gọi produce
                "task_failure_count": task_failure_count,          # Số lỗi trong task (simulated + producer errors)
                "total_count": total_records_in_task,              # Tổng số record được giao
                "elapsed_time": elapsed_time,
                "rate": rate
            }

        except Exception as e:
            # Lỗi nghiêm trọng xảy ra ngoài vòng lặp (ít khả năng nếu producer đã tạo thành công)
            self.producer_end_times[producer_id] = time.time()
            elapsed_time = self.producer_end_times.get(producer_id, time.time()) - self.producer_start_times.get(producer_id, time.time())

            logger.error(f"Producer {producer_id} gặp lỗi nghiêm trọng ngoài vòng lặp: {str(e)}")

            # Giả sử tất cả đều fail nếu có lỗi nghiêm trọng ở đây
            fail_count_on_error = total_records_in_task
            self.metrics.kafka_records_failed.inc(fail_count_on_error)

            return {
                "producer_id": producer_id,
                "attempted_success_count": 0,
                "task_failure_count": fail_count_on_error,
                "total_count": fail_count_on_error,
                "elapsed_time": elapsed_time,
                "error": str(e)
            }
        finally:
            # Đảm bảo producer được giảm đi trong metrics ngay cả khi có lỗi
            try:
                self.metrics.kafka_producer_active.dec()
                logger.debug(f"Decremented active producer count for {producer_id}")
            except Exception as final_err:
                logger.warning(f"Producer {producer_id}: Lỗi nhỏ khi cleanup metrics: {final_err}")


    def split_data(self, data: List[Dict[str, Any]], num_parts: int) -> List[List[Dict[str, Any]]]:
        """
        Chia danh sách dữ liệu thành nhiều phần để xử lý song song.
        Cải thiện để chia đều hơn.

        Args:
            data: Danh sách dữ liệu cần chia.
            num_parts: Số phần mong muốn (số producer).

        Returns:
            List[List[Dict[str, Any]]]: Danh sách các phần đã chia.
        """
        n = len(data)
        if not data or num_parts <= 0:
            return []
        if num_parts == 1:
            return [data]

        # Đảm bảo số phần không lớn hơn số lượng dữ liệu
        actual_num_parts = min(num_parts, n)

        # Tính kích thước cơ bản và số phần dư
        base_size = n // actual_num_parts
        remainder = n % actual_num_parts

        chunks = []
        start_index = 0
        for i in range(actual_num_parts):
            # Những phần đầu tiên sẽ lớn hơn 1 đơn vị nếu có phần dư
            end_index = start_index + base_size + (1 if i < remainder else 0)
            chunks.append(data[start_index:end_index])
            start_index = end_index

        logger.info(f"Chia {n:,} bản ghi thành {len(chunks)} phần (target: {num_parts} parts)")
        # Log kích thước của từng chunk để kiểm tra
        for i, chunk in enumerate(chunks):
            logger.debug(f"  Chunk {i}: {len(chunk):,} bản ghi")

        return chunks

    def send_to_kafka(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Gửi dữ liệu vào Kafka sử dụng nhiều producer chạy song song.

        Args:
            data: Dữ liệu cần gửi.

        Returns:
            Dict[str, Any]: Kết quả tổng hợp của việc gửi dữ liệu.
        """
        total_records_to_send = len(data)
        if not data:
            logger.warning(f"Không có dữ liệu để gửi vào topic {self.kafka_topic}")
            return {
                "processed_records": 0,
                "delivered_success_count": 0,
                "total_failure_count": 0,
                "success_rate_percent": 0.0,
                "elapsed_time": 0,
                "overall_processing_rate": 0,
                "producer_results": []
            }

        # Reset metrics thành công/thất bại trước khi chạy batch mới (quan trọng!)
        # Cần đảm bảo cách reset này phù hợp với thư viện metrics bạn dùng
        # Nếu không có hàm reset, bạn cần lấy giá trị trước và sau để tính delta
        # self.metrics.kafka_records_sent.clear() # Giả sử có hàm clear
        # self.metrics.kafka_records_failed.clear() # Giả sử có hàm clear
        # Hoặc lấy giá trị ban đầu:
        initial_sent = self.metrics.kafka_records_sent.collect()[0].samples[0].value
        initial_failed = self.metrics.kafka_records_failed.collect()[0].samples[0].value

        self.start_time = time.time()

        # Chia dữ liệu
        data_chunks = self.split_data(data, self.kafka_num_producers)
        actual_producers_used = len(data_chunks) # Số producer thực sự cần dùng

        threads = []
        results = [] # Sử dụng list để đảm bảo thứ tự nếu cần, hoặc dict

        logger.info(f"Bắt đầu gửi {total_records_to_send:,} bản ghi vào topic '{self.kafka_topic}' sử dụng {actual_producers_used} producer threads.")

        # Khởi tạo và chạy các threads
        for i in range(actual_producers_used):
            # Gán producer_id duy nhất cho mỗi thread/task
            producer_id = i
            chunk = data_chunks[i]
            # Sử dụng lambda để truyền đúng chunk và producer_id vào target
            thread = threading.Thread(
                target=lambda p_id=producer_id, ch=chunk: results.append(self.producer_task(ch, p_id)),
                name=f"ProducerThread-{producer_id}"
            )
            threads.append(thread)
            thread.start()

        # Đợi tất cả các thread producer hoàn thành
        logger.info(f"Đang chờ {len(threads)} producer threads hoàn thành...")
        for thread in threads:
            thread.join()
        logger.info("Tất cả producer threads đã hoàn thành.")

        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time

        # --- Tổng hợp kết quả cuối cùng ---
        # Lấy số liệu cuối cùng từ metrics (sau khi tất cả callback đã được xử lý)
        final_sent = self.metrics.kafka_records_sent.collect()[0].samples[0].value
        final_failed = self.metrics.kafka_records_failed.collect()[0].samples[0].value

        # Tính số lượng thành công và thất bại trong lần chạy này
        delivered_success_in_run = final_sent - initial_sent
        failures_in_run = final_failed - initial_failed

        # Tổng số record thực sự được xử lý bởi các producer (bằng tổng số record ban đầu)
        total_processed_records = total_records_to_send

        # Sử dụng context manager cho thời gian xử lý tổng thể của ứng dụng
        with self.metrics.app_total_processing_time.time():
            # Rate tổng thể dựa trên số record đã xử lý / tổng thời gian
            overall_processing_rate = total_processed_records / elapsed_time if elapsed_time > 0 else 0
            self.metrics.kafka_send_rate.set(overall_processing_rate) # Metric này thể hiện processing rate

            # Tính tỷ lệ thành công cuối cùng dựa trên số liệu delivery thực tế
            if total_processed_records > 0:
                actual_success_rate_percent = (delivered_success_in_run / total_processed_records) * 100
                self.metrics.kafka_success_rate.set(actual_success_rate_percent)
            else:
                actual_success_rate_percent = 100.0 if failures_in_run == 0 else 0.0

            logger.success(f"--- KẾT QUẢ GỬI KAFKA ---")
            logger.success(f"Tổng số records xử lý: {total_processed_records:,}")
            logger.success(f"Số records gửi thành công (ACKed): {delivered_success_in_run:,}")
            logger.success(f"Tổng số lỗi (Simulated+Delivery+Producer): {failures_in_run:,}")
            logger.success(f"Tỷ lệ thành công cuối cùng: {actual_success_rate_percent:.2f}%")
            logger.success(f"Tổng thời gian gửi Kafka: {format_duration(elapsed_time)} ({elapsed_time:.2f}s)")
            logger.success(f"Tốc độ xử lý Kafka tổng thể: {overall_processing_rate:.2f} bản ghi/giây")


            return {
                "processed_records": total_processed_records,
                "delivered_success_count": delivered_success_in_run,
                "total_failure_count": failures_in_run,
                "success_rate_percent": actual_success_rate_percent,
                "elapsed_time": elapsed_time,
                "overall_processing_rate": overall_processing_rate,
                "producer_results": results # Giữ kết quả chi tiết từ mỗi producer task
            }

    def start_metrics_monitor(self):
        """Khởi động thread giám sát tài nguyên và metrics (nếu bật)."""
        if not METRICS_ENABLED:
            logger.info("Metrics monitor không khởi động vì metrics bị vô hiệu hóa.")
            return

        def monitor_task():
            logger.info(f"Bắt đầu theo dõi metrics mỗi {self.track_interval} giây tại cổng {self.prometheus_port}")
            while self.running:
                try:
                    # Theo dõi tài nguyên CPU/Memory
                    self.metrics.track_memory_and_cpu()

                    # Cập nhật các metrics tổng hợp khác nếu cần
                    # Ví dụ: tính lại success rate dựa trên giá trị hiện tại
                    self.metrics.update_kafka_success_rate() # Cần hàm này trong kafka_metrics

                    # Log một số metrics định kỳ (tùy chọn)
                    # current_sent = self.metrics.kafka_records_sent.collect()[0].samples[0].value
                    # current_failed = self.metrics.kafka_records_failed.collect()[0].samples[0].value
                    # logger.debug(f"Metrics Monitor: Sent={current_sent}, Failed={current_failed}")

                except Exception as e:
                    logger.error(f"Lỗi trong thread giám sát metrics: {str(e)}")

                # Chờ đến interval tiếp theo
                time.sleep(self.track_interval)
            logger.info("Thread giám sát metrics đã dừng.")

        self.running = True
        self.monitor_thread = threading.Thread(target=monitor_task, name="MetricsMonitorThread")
        self.monitor_thread.daemon = True # Tự thoát khi chương trình chính thoát
        self.monitor_thread.start()

    def stop_metrics_monitor(self):
        """Dừng thread giám sát metrics."""
        if self.running:
            logger.info("Đang dừng thread giám sát metrics...")
            self.running = False
            if self.monitor_thread and self.monitor_thread.is_alive():
                try:
                    self.monitor_thread.join(timeout=self.track_interval + 1) # Chờ tối đa một interval
                    if self.monitor_thread.is_alive():
                        logger.warning("Thread giám sát metrics không dừng kịp thời.")
                except Exception as e:
                    logger.error(f"Lỗi khi dừng thread giám sát metrics: {e}")
            self.monitor_thread = None


    def run(self) -> Dict[str, Any]:
        """
        Chạy toàn bộ quy trình: đọc từ MongoDB, gửi lên Kafka và theo dõi.

        Returns:
            Dict[str, Any]: Kết quả thực hiện tổng thể.
        """
        run_start_time = time.time()
        final_result = {}

        try:
            # Khởi động thread giám sát metrics (nếu bật)
            self.start_metrics_monitor()

            logger.info("=== BẮT ĐẦU QUY TRÌNH: MongoDB -> Kafka ===")

            # Bước 1: Đọc dữ liệu từ MongoDB
            logger.info("--- Bước 1: Đọc dữ liệu từ MongoDB ---")
            mongo_start = time.time()
            data = self.load_from_mongodb()
            mongo_elapsed = time.time() - mongo_start

            if not data:
                logger.error("Không đọc được dữ liệu từ MongoDB hoặc collection rỗng/không khớp query.")
                final_result = {
                    "status": "error",
                    "message": "Không đọc được dữ liệu từ MongoDB",
                    "mongo_elapsed_time": mongo_elapsed,
                    "total_elapsed_time": time.time() - run_start_time
                }
                return final_result # Kết thúc sớm

            total_records_read = len(data)
            read_rate = total_records_read / mongo_elapsed if mongo_elapsed > 0 else 0
            logger.info(f"Đã đọc {total_records_read:,} bản ghi từ MongoDB (Tốc độ: {read_rate:.2f} recs/s)")


            # Bước 2: Gửi dữ liệu lên Kafka
            logger.info("--- Bước 2: Gửi dữ liệu lên Kafka ---")
            kafka_start = time.time()
            kafka_result = self.send_to_kafka(data) # Hàm này đã được cập nhật để trả về kết quả chi tiết
            kafka_elapsed = time.time() - kafka_start

            # Tổng hợp kết quả cuối cùng
            total_elapsed = time.time() - run_start_time

            # Lấy các giá trị chính từ kafka_result
            delivered_success = kafka_result.get("delivered_success_count", 0)
            total_failures = kafka_result.get("total_failure_count", 0)
            final_success_rate = kafka_result.get("success_rate_percent", 0.0)
            kafka_processing_rate = kafka_result.get("overall_processing_rate", 0.0)

            final_result = {
                "status": "success", # Coi là success nếu chạy hết, ngay cả khi có lỗi message
                "total_records_read_from_mongo": total_records_read,
                "kafka_records_processed": kafka_result.get("processed_records", 0),
                "kafka_records_delivered_success": delivered_success,
                "kafka_total_failures": total_failures,
                "kafka_final_success_rate_percent": final_success_rate,
                "mongo_elapsed_time_sec": mongo_elapsed,
                "kafka_elapsed_time_sec": kafka_elapsed,
                "total_elapsed_time_sec": total_elapsed,
                "mongo_read_rate_rps": read_rate,
                "kafka_processing_rate_rps": kafka_processing_rate,
                "overall_rate_rps": total_records_read / total_elapsed if total_elapsed > 0 else 0,
                "simulation_enabled": self.simulate_failure_rate > 0,
                "simulation_rate": self.simulate_failure_rate if self.simulate_failure_rate > 0 else None,
                # "producer_details": kafka_result.get("producer_results", []) # Tùy chọn: thêm chi tiết producer
            }

            # Hiển thị báo cáo kết quả tổng thể
            logger.success("=== KẾT QUẢ THỰC HIỆN TỔNG THỂ ===")
            logger.success(f"Đọc từ MongoDB: {final_result['total_records_read_from_mongo']:,} records")
            logger.success(f"Kafka - Đã xử lý: {final_result['kafka_records_processed']:,} records")
            logger.success(f"Kafka - Gửi thành công (ACKed): {final_result['kafka_records_delivered_success']:,} records")
            logger.success(f"Kafka - Tổng lỗi (Simulated+Delivery+Producer): {final_result['kafka_total_failures']:,} records")
            logger.success(f"Kafka - Tỷ lệ thành công cuối cùng: {final_result['kafka_final_success_rate_percent']:.2f}%")
            if final_result['simulation_enabled']:
                logger.warning(f"*** Chế độ mô phỏng lỗi Kafka đã được bật (Tỷ lệ: {final_result['simulation_rate']*100:.1f}%) ***")
            logger.success(f"Thời gian đọc MongoDB: {format_duration(final_result['mongo_elapsed_time_sec'])}")
            logger.success(f"Thời gian xử lý Kafka: {format_duration(final_result['kafka_elapsed_time_sec'])}")
            logger.success(f"Tổng thời gian thực thi: {format_duration(final_result['total_elapsed_time_sec'])}")
            logger.success(f"Tốc độ đọc MongoDB: {final_result['mongo_read_rate_rps']:.2f} bản ghi/giây")
            logger.success(f"Tốc độ xử lý Kafka: {final_result['kafka_processing_rate_rps']:.2f} bản ghi/giây")
            logger.success(f"Tốc độ tổng thể (Mongo->Kafka): {final_result['overall_rate_rps']:.2f} bản ghi/giây")

            # Thông tin về trace_id
            logger.info("---")
            logger.info("Gợi ý truy vấn Elasticsearch/Kibana:")
            logger.info(f"  - Tìm một bản ghi cụ thể: trace_id:\"<your-uuid>\"")
            logger.info(f"  - Tìm lỗi mô phỏng: status:simulated_error")
            logger.info(f"  - Tìm lỗi delivery thực tế: status:delivery_error")
            logger.info(f"  - Tìm lỗi producer: status:producer_error")
            logger.info(f"  - Tìm tất cả lỗi của một trace_id: trace_id:\"<your-uuid>\" AND status:(simulated_error OR delivery_error OR producer_error)")
            logger.info(f"  - Tìm tất cả các bước của một _id: _id:\"<mongodb_object_id>\"")

            # Tìm Producer nhanh nhất/chậm nhất (dựa trên thời gian hoàn thành task)
            producer_results_list = kafka_result.get("producer_results", [])
            producer_times = [ (r.get("producer_id"), r.get("elapsed_time", 0))
                            for r in producer_results_list if r.get("elapsed_time", 0) > 0 ]

            if producer_times:
                try:
                    fastest = min(producer_times, key=lambda x: x[1])
                    slowest = max(producer_times, key=lambda x: x[1])
                    logger.info(f"Producer task nhanh nhất: #{fastest[0]} ({format_duration(fastest[1])})")
                    logger.info(f"Producer task chậm nhất: #{slowest[0]} ({format_duration(slowest[1])})")
                except ValueError:
                    logger.info("Không đủ dữ liệu thời gian producer để so sánh.")


            return final_result

        except Exception as e:
            logger.error(f"Lỗi không mong muốn trong quá trình chạy chính: {str(e)}", exception=True)
            final_result = {
                "status": "error",
                "message": f"Lỗi không mong muốn: {str(e)}",
                "total_elapsed_time": time.time() - run_start_time
            }
            return final_result

        finally:
            # Dừng thread giám sát metrics khi kết thúc (thành công hoặc lỗi)
            self.stop_metrics_monitor()
            logger.info("=== KẾT THÚC QUY TRÌNH ===")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="MongoDB to Kafka Data Pipeline with Monitoring and Error Simulation")
    parser.add_argument("--config", help="Path to the configuration file (e.g., config.ini)")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Create and run the monitor instance
    monitor = MongoToKafkaMonitor(config)
    result = monitor.run()

    # Exit code based on final status
    if result.get("status") == "success":
        # Kiểm tra thêm nếu muốn coi có lỗi message là thất bại
        if result.get("kafka_total_failures", 0) > 0:
            logger.warning(f"Quy trình hoàn thành nhưng có {result['kafka_total_failures']} lỗi khi gửi message.")
            # return 1 # Trả về lỗi nếu có bất kỳ message nào fail
            return 0 # Hoặc trả về 0 vì quy trình đã chạy hết
        else:
            logger.success("Quy trình hoàn thành thành công không có lỗi message.")
            return 0 # Thành công hoàn toàn
    else:
        logger.error(f"Quy trình thất bại với lỗi: {result.get('message', 'Unknown error')}")
        return 1 # Thất bại

if __name__ == "__main__":
    sys.exit(main())