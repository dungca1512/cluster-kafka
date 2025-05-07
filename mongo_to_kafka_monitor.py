import json
import threading
import time
import os
import sys
import argparse
import configparser
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Callable, Optional

from loguru import logger
from confluent_kafka import Producer
from pymongo import MongoClient

# Import custom modules
from kafka_metrics import setup_metrics, get_metrics

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
    level="INFO"
)

# Mặc định
DEFAULT_CONFIG = {
    "mongodb": {
        "uri": "mongodb://localhost:27017",
        "db": "user_fcm",
        "collection": "users_fcm",
        "query": "{}",
        "projection": "{}",
        "limit": "0",
        "batch_size": "1000"
    },
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic": "test",
        "num_producers": "10",
        "batch_size": "5000",
        "linger_ms": "5"
    },
    "monitoring": {
        "prometheus_port": "8000",
        "track_interval_seconds": "5",
        "detailed_producer_metrics": "true"
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
            config[section][key] = value
    
    # Tải cấu hình từ file nếu có
    if config_path and os.path.exists(config_path):
        logger.info(f"Đang tải cấu hình từ file: {config_path}")
        config.read(config_path)
    else:
        logger.info("Sử dụng cấu hình mặc định")
    
    return config

def format_duration(seconds: float) -> str:
    """
    Định dạng thời gian từ giây sang chuỗi dễ đọc
    
    Args:
        seconds: Số giây
    
    Returns:
        str: Chuỗi thời gian đã định dạng
    """
    duration = timedelta(seconds=seconds)
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

class MongoToKafkaMonitor:
    """
    Lớp chính để quản lý quá trình lấy dữ liệu từ MongoDB và gửi lên Kafka
    với theo dõi metrics và trace_id
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
        self.mongo_limit = int(config['mongodb']['limit'])
        self.mongo_batch_size = int(config['mongodb']['batch_size'])
        
        # Cấu hình Kafka
        self.kafka_bootstrap_servers = config['kafka']['bootstrap_servers']
        self.kafka_topic = config['kafka']['topic']
        self.kafka_num_producers = int(config['kafka']['num_producers'])
        self.kafka_batch_size = int(config['kafka']['batch_size'])
        self.kafka_linger_ms = int(config['kafka']['linger_ms'])
        
        # Cấu hình giám sát
        self.prometheus_port = int(config['monitoring']['prometheus_port'])
        self.track_interval = int(config['monitoring']['track_interval_seconds'])
        
        # Khởi tạo metrics
        self.metrics = setup_metrics(port=self.prometheus_port)
        
        # Biến theo dõi
        self.start_time = None
        self.end_time = None
        self.producer_start_times = [0] * self.kafka_num_producers
        self.producer_end_times = [0] * self.kafka_num_producers
        self.producer_records = [0] * self.kafka_num_producers
        
        # Biến để theo dõi quá trình metrics
        self.monitor_thread = None
        self.running = False
        
        logger.info(f"Đã khởi tạo MongoToKafkaMonitor với {self.kafka_num_producers} producers")
    
    def load_from_mongodb(self) -> List[Dict[str, Any]]:
        """
        Tải dữ liệu từ MongoDB
        
        Returns:
            List[Dict[str, Any]]: Danh sách các bản ghi (giới hạn 1000 bản ghi)
        """
        start_time = time.time()
        logger.info(f"Kết nối đến MongoDB: {self.mongo_uri}")
        
        try:
            # Sử dụng context manager with để tự động đóng kết nối
            with MongoClient(self.mongo_uri) as client:
                # Đánh dấu đã kết nối thành công
                client.admin.command('ping')
                db = client[self.mongo_db]
                collection = db[self.mongo_collection]
                
                # Đếm tổng số bản ghi trước khi truy vấn
                if self.mongo_query:
                    total_records = collection.count_documents(self.mongo_query)
                else:
                    total_records = collection.estimated_document_count()
                
                logger.info(f"Tìm thấy tổng cộng {total_records:,} bản ghi trong collection {self.mongo_collection}")
                
                # Áp dụng giới hạn 1000 bản ghi
                limit = min(1000, self.mongo_limit if self.mongo_limit > 0 else total_records)
                logger.info(f"Giới hạn truy vấn tối đa {limit:,} bản ghi")
                
                # Chuẩn bị dữ liệu trả về
                data = []
                
                # Sử dụng metrics để theo dõi
                with self.metrics.mongodb_read_time.time():
                    # Sử dụng cursor với batch_size để xử lý hiệu quả
                    cursor = collection.find(
                        filter=self.mongo_query,
                        projection=self.mongo_projection
                    ).batch_size(self.mongo_batch_size).limit(limit)
                    
                    # Đọc dữ liệu từ cursor
                    for record in cursor:
                        # Chuyển ObjectId thành string
                        if "_id" in record:
                            record["_id"] = str(record["_id"])
                        
                        # Thêm trace_id vào mỗi bản ghi ngay từ nguồn
                        record["trace_id"] = generate_trace_id()
                        record["_mongodb_timestamp"] = datetime.now().isoformat()
                        record["_source_system"] = "mongodb"
                            
                        data.append(record)
                        
                        # Log chi tiết cho mỗi bản ghi đã đọc
                        logger.debug(f"Đã đọc bản ghi từ MongoDB [_id={record['_id']}] [trace_id={record['trace_id']}]")
                        
                        # Cập nhật metrics theo số lượng bản ghi đọc được
                        self.metrics.mongodb_records_read.inc()
                
                # Đánh dấu quá trình đọc dữ liệu đã hoàn thành
                elapsed_time = time.time() - start_time
                
                if elapsed_time > 0:
                    read_rate = len(data) / elapsed_time
                    self.metrics.mongodb_read_rate.set(read_rate)
                    logger.info(f"Đã đọc {len(data):,} bản ghi trong {elapsed_time:.2f}s ({read_rate:.2f} bản ghi/giây)")
                else:
                    logger.info(f"Đã đọc {len(data):,} bản ghi")
                
                return data
                
        except Exception as e:
            logger.error(f"Lỗi khi kết nối đến MongoDB: {str(e)}")
            self.metrics.mongodb_connection_errors.inc()
            return []
    
    def create_kafka_producer(self, producer_id: int) -> Producer:
        """
        Tạo một Kafka producer
        
        Args:
            producer_id: ID của producer
        
        Returns:
            Producer: Đối tượng producer
        """
        try:
            conf = {
                "bootstrap.servers": self.kafka_bootstrap_servers,
                "client.id": f"producer-{producer_id}",
                "queue.buffering.max.messages": 100000,
                "batch.num.messages": 5000,
                "linger.ms": self.kafka_linger_ms,
            }
            
            producer = Producer(conf)
            
            # Theo dõi số lượng producer đang hoạt động
            self.metrics.kafka_producer_active.inc()
            
            # Đăng ký producer với metrics
            self.metrics.register_producer(producer_id)
            
            return producer
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo Kafka producer {producer_id}: {str(e)}")
            self.metrics.kafka_connection_errors.inc()
            raise
    
    def delivery_report(self, err, msg):
        """
        Callback được gọi khi message được gửi đi hoặc gặp lỗi
        
        Args:
            err: Lỗi nếu có
            msg: Message đã gửi
        """
        # Trích xuất trace_id từ headers
        trace_id = None
        record_id = None
        producer_id = None
        
        try:
            if msg.headers():
                for header in msg.headers():
                    if header[0] == "trace_id":
                        trace_id = header[1].decode('utf-8')
                    elif header[0] == "record_id":
                        record_id = header[1].decode('utf-8')
                    elif header[0] == "producer_id":
                        producer_id = header[1].decode('utf-8')
        except Exception as e:
            logger.warning(f"Không thể đọc headers: {str(e)}")
        
        if err is not None:
            logger.error(f"KAFKA_DELIVERY [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=error] [error={err}]")
            self.metrics.kafka_records_failed.inc()
        else:
            logger.info(f"KAFKA_DELIVERY [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=success] [partition={msg.partition()}]")
            self.metrics.kafka_records_sent.inc()
    
    def producer_task(self, data: List[Dict[str, Any]], producer_id: int) -> Dict[str, Any]:
        """
        Hàm được chạy trong từng thread để gửi dữ liệu 
        
        Args:
            data: Dữ liệu cần gửi
            producer_id: ID của producer
            
        Returns:
            Dict[str, Any]: Kết quả gửi dữ liệu
        """
        if not data:
            return {
                "producer_id": producer_id,
                "success_count": 0,
                "total_count": 0,
                "elapsed_time": 0
            }
        
        # Đánh dấu thời gian bắt đầu
        self.producer_start_times[producer_id] = time.time()
        producer = self.create_kafka_producer(producer_id)
        success_count = 0
        
        try:
            logger.info(f"Producer {producer_id} bắt đầu gửi {len(data)} bản ghi")
            
            # Theo dõi batch
            with self.metrics.kafka_batch_processing_time.time():
                # Đánh dấu số lượng bản ghi đang xử lý
                self.metrics.begin_batch_processing(len(data))
                
                for idx, record in enumerate(data):
                    try:
                        # Kiểm tra và đảm bảo có trace_id
                        if "trace_id" not in record:
                            record["trace_id"] = generate_trace_id()
                        
                        # Thêm metadata về thời gian và nguồn
                        current_time = datetime.now().isoformat()
                        record["_kafka_timestamp"] = current_time
                        record["_kafka_producer_id"] = producer_id
                        record["_processing_phase"] = "kafka_producer"
                        
                        # Thêm trường status để theo dõi trạng thái xử lý
                        record["status"] = "sent_to_kafka"
                        
                        # Log chi tiết cho mỗi bản ghi
                        record_id = record.get("_id", "unknown")
                        trace_id = record["trace_id"]
                        logger.info(f"KAFKA_EVENT [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=sending]")
                        
                        # Chuyển record thành JSON
                        record_json = json.dumps(record)
                        
                        # Gửi message vào topic với headers chứa trace_id
                        producer.produce(
                            topic=self.kafka_topic,
                            value=record_json.encode('utf-8'),
                            headers={
                                "trace_id": record["trace_id"].encode('utf-8'),
                                "record_id": str(record_id).encode('utf-8'),
                                "producer_id": str(producer_id).encode('utf-8'),
                                "timestamp": current_time.encode('utf-8'),
                                "source": "mongodb_to_kafka".encode('utf-8')
                            },
                            callback=self.delivery_report
                        )
                        
                        # Poll để xử lý các sự kiện và callback
                        producer.poll(0)
                        
                        # Cập nhật số lượng message đã gửi thành công
                        success_count += 1
                        self.producer_records[producer_id] = success_count
                        
                        # Log thành công
                        logger.info(f"KAFKA_EVENT [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=sent]")
                        
                        # Flush định kỳ
                        if (idx + 1) % 1000 == 0:
                            producer.flush()
                            
                            # Log tiến độ và cập nhật metrics
                            elapsed = time.time() - self.producer_start_times[producer_id]
                            if elapsed > 0:
                                rate = (idx + 1) / elapsed
                                logger.debug(f"Producer {producer_id}: Đã gửi {idx + 1}/{len(data)} bản ghi (Rate: {rate:.2f} records/sec)")
                                
                                # Cập nhật metrics cho producer
                                self.metrics.producer_send_rate[producer_id].set(rate)
                    
                    except Exception as e:
                        record_id = record.get("_id", "unknown")
                        trace_id = record.get("trace_id", "unknown")
                        logger.error(f"KAFKA_EVENT [producer={producer_id}] [_id={record_id}] [trace_id={trace_id}] [status=error] [error={str(e)}]")
                        # Cập nhật trạng thái lỗi cho bản ghi này (cho log local)
                        try:
                            record["status"] = "error"
                            record["error_message"] = str(e)
                            record["error_timestamp"] = datetime.now().isoformat()
                        except:
                            pass
            
            # Flush lần cuối
            producer.flush()
            
            # Tính toán kết quả
            self.producer_end_times[producer_id] = time.time()
            elapsed_time = self.producer_end_times[producer_id] - self.producer_start_times[producer_id]
            rate = len(data) / elapsed_time if elapsed_time > 0 else 0
            
            # Cập nhật metrics cho producer
            self.metrics.update_producer_metrics(
                producer_id=producer_id, 
                records_sent=success_count,
                elapsed_time=elapsed_time
            )
            
            # Đánh dấu kết thúc batch
            self.metrics.end_batch_processing(success_count=success_count, fail_count=(len(data) - success_count))
            
            logger.info(f"Producer {producer_id} hoàn thành: {success_count}/{len(data)} bản ghi "
                    f"trong {elapsed_time:.2f}s ({rate:.2f} bản ghi/giây)")
            
            return {
                "producer_id": producer_id,
                "success_count": success_count,
                "total_count": len(data),
                "elapsed_time": elapsed_time,
                "rate": rate
            }
        
        except Exception as e:
            self.producer_end_times[producer_id] = time.time()
            elapsed_time = self.producer_end_times[producer_id] - self.producer_start_times[producer_id]
            
            logger.error(f"Producer {producer_id} gặp lỗi: {str(e)}")
            
            # Đánh dấu kết thúc batch với lỗi
            self.metrics.end_batch_processing(success_count=success_count, fail_count=(len(data) - success_count))
            
            return {
                "producer_id": producer_id,
                "success_count": success_count,
                "total_count": len(data),
                "elapsed_time": elapsed_time,
                "error": str(e)
            }
        
        finally:
            try:
                producer.flush()
                # Giảm số lượng producer đang hoạt động
                self.metrics.kafka_producer_active.dec()
            except Exception:
                pass
    
    def split_data(self, data: List[Dict[str, Any]], num_parts: int) -> List[List[Dict[str, Any]]]:
        """
        Chia danh sách dữ liệu thành nhiều phần để xử lý song song
        
        Args:
            data: Danh sách dữ liệu cần chia
            num_parts: Số phần cần chia
            
        Returns:
            List[List[Dict[str, Any]]]: Danh sách các phần đã chia
        """
        if not data:
            return []
        
        if num_parts <= 1:
            return [data]
        
        # Điều chỉnh số phần nếu ít hơn dữ liệu
        num_parts = min(num_parts, len(data))
        
        # Chia dữ liệu thành các phần bằng nhau
        chunk_size = len(data) // num_parts
        chunks = []
        
        for i in range(num_parts):
            # Phần cuối cùng sẽ chứa phần dư
            if i == num_parts - 1:
                chunks.append(data[i * chunk_size:])
            else:
                chunks.append(data[i * chunk_size:(i + 1) * chunk_size])
        
        logger.debug(f"Chia {len(data)} bản ghi thành {len(chunks)} phần")
        return chunks
    
    def send_to_kafka(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Gửi dữ liệu vào Kafka sử dụng nhiều producer chạy song song
        
        Args:
            data: Dữ liệu cần gửi
            
        Returns:
            Dict[str, Any]: Kết quả gửi dữ liệu
        """
        if not data:
            logger.warning(f"Không có dữ liệu để gửi vào topic {self.kafka_topic}")
            return {
                "success_count": 0,
                "total_count": 0,
                "elapsed_time": 0,
                "rate": 0
            }
        
        # Khởi tạo thời gian bắt đầu
        self.start_time = time.time()
        
        # Chia dữ liệu thành các phần cho các producer
        data_chunks = self.split_data(data, self.kafka_num_producers)
        num_chunks = len(data_chunks)
        actual_producers = min(self.kafka_num_producers, num_chunks)
        
        # Chuẩn bị các biến theo dõi
        threads = []
        results = []
        
        logger.info(f"Bắt đầu gửi {len(data)} bản ghi vào topic {self.kafka_topic} sử dụng {actual_producers} producer")
        
        # Tạo và khởi động các thread
        for i in range(num_chunks):
            thread = threading.Thread(
                target=lambda idx=i, chunk=data_chunks[i]: 
                    results.append(self.producer_task(chunk, idx))
            )
            threads.append(thread)
            thread.start()
        
        # Đợi tất cả thread hoàn thành
        for thread in threads:
            thread.join()
        
        # Tính toán kết quả
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        
        # Cập nhật metrics cho toàn bộ quá trình
        with self.metrics.app_total_processing_time.time():
            success_count = sum(r.get("success_count", 0) for r in results)
            total_count = sum(r.get("total_count", 0) for r in results)
            
            # Tính tốc độ gửi
            rate = total_count / elapsed_time if elapsed_time > 0 else 0
            self.metrics.kafka_send_rate.set(rate)
            
            # Cập nhật tỷ lệ thành công
            if total_count > 0:
                success_rate = (success_count / total_count) * 100
                self.metrics.kafka_success_rate.set(success_rate)
            
            logger.success(f"Hoàn thành gửi {success_count}/{total_count} bản ghi "
                        f"trong {elapsed_time:.2f}s ({rate:.2f} bản ghi/giây)")
            
            return {
                "success_count": success_count,
                "total_count": total_count,
                "elapsed_time": elapsed_time,
                "rate": rate,
                "producer_results": results
            }
    
    def start_metrics_monitor(self):
        """Khởi động thread giám sát tài nguyên và metrics"""
        def monitor_task():
            """Task chạy trong monitor thread"""
            logger.info(f"Bắt đầu theo dõi metrics mỗi {self.track_interval} giây")
            
            while self.running:
                try:
                    # Theo dõi tài nguyên
                    self.metrics.track_memory_and_cpu()
                    
                    # Cập nhật metrics cho Kafka
                    if any(self.producer_records):
                        total_records = sum(self.producer_records)
                        if self.start_time and time.time() - self.start_time > 0:
                            current_rate = total_records / (time.time() - self.start_time)
                            self.metrics.kafka_send_rate.set(current_rate)
                    
                    # Cập nhật tỷ lệ thành công
                    self.metrics.update_kafka_success_rate()
                    
                except Exception as e:
                    logger.error(f"Lỗi khi cập nhật metrics: {str(e)}")
                
                # Chờ đến interval tiếp theo
                time.sleep(self.track_interval)
        
        # Khởi động thread giám sát
        self.running = True
        self.monitor_thread = threading.Thread(target=monitor_task)
        self.monitor_thread.daemon = True  # Thread daemon sẽ tự động kết thúc khi main thread kết thúc
        self.monitor_thread.start()
    
    def stop_metrics_monitor(self):
        """Dừng thread giám sát"""
        if self.running:
            self.running = False
            
            if self.monitor_thread:
                try:
                    self.monitor_thread.join(timeout=5)
                except Exception:
                    pass
                
                logger.info("Đã dừng thread giám sát metrics")
    
    def run(self) -> Dict[str, Any]:
        """
        Chạy toàn bộ quy trình: đọc từ MongoDB, gửi lên Kafka và theo dõi metrics
        
        Returns:
            Dict[str, Any]: Kết quả thực hiện
        """
        try:
            # Khởi động thread giám sát metrics
            self.start_metrics_monitor()
            
            logger.info("=== BẮT ĐẦU QUY TRÌNH ĐỌC MONGODB VÀ GỬI LÊN KAFKA ===")
            
            # Bước 1: Đọc dữ liệu từ MongoDB
            logger.info("Bước 1: Đọc dữ liệu từ MongoDB")
            mongo_start = time.time()
            data = self.load_from_mongodb()
            mongo_elapsed = time.time() - mongo_start
            
            if not data:
                logger.error("Không thể đọc dữ liệu từ MongoDB hoặc không có dữ liệu")
                return {
                    "status": "error",
                    "message": "Không thể đọc dữ liệu từ MongoDB hoặc không có dữ liệu",
                    "mongo_elapsed_time": mongo_elapsed
                }
            
            # Bước 2: Gửi dữ liệu lên Kafka
            logger.info("Bước 2: Gửi dữ liệu lên Kafka")
            kafka_start = time.time()
            kafka_result = self.send_to_kafka(data)
            kafka_elapsed = time.time() - kafka_start
            
            # Tổng hợp kết quả
            total_elapsed = time.time() - mongo_start
            
            result = {
                "status": "success",
                "total_records": len(data),
                "sent_records": kafka_result["success_count"],
                "failed_records": kafka_result["total_count"] - kafka_result["success_count"],
                "success_rate": (kafka_result["success_count"] / kafka_result["total_count"]) * 100 if kafka_result["total_count"] > 0 else 0,
                "mongo_elapsed_time": mongo_elapsed,
                "kafka_elapsed_time": kafka_elapsed,
                "total_elapsed_time": total_elapsed,
                "read_rate": len(data) / mongo_elapsed if mongo_elapsed > 0 else 0,
                "send_rate": kafka_result["rate"],
                "overall_rate": len(data) / total_elapsed if total_elapsed > 0 else 0
            }
            
            # Hiển thị báo cáo kết quả
            logger.success("=== KẾT QUẢ THỰC HIỆN ===")
            logger.success(f"Tổng số bản ghi: {result['total_records']:,}")
            logger.success(f"Số bản ghi đã gửi thành công: {result['sent_records']:,}")
            logger.success(f"Số bản ghi gửi thất bại: {result['failed_records']:,}")
            logger.success(f"Tỷ lệ thành công: {result['success_rate']:.2f}%")
            logger.success(f"Thời gian đọc MongoDB: {format_duration(result['mongo_elapsed_time'])} ({result['mongo_elapsed_time']:.2f}s)")
            logger.success(f"Thời gian gửi Kafka: {format_duration(result['kafka_elapsed_time'])} ({result['kafka_elapsed_time']:.2f}s)")
            logger.success(f"Tổng thời gian: {format_duration(result['total_elapsed_time'])} ({result['total_elapsed_time']:.2f}s)")
            logger.success(f"Tốc độ đọc: {result['read_rate']:.2f} bản ghi/giây")
            logger.success(f"Tốc độ gửi: {result['send_rate']:.2f} bản ghi/giây")
            logger.success(f"Tốc độ tổng thể: {result['overall_rate']:.2f} bản ghi/giây")
            
            # Thông tin về trace_id để theo dõi
            logger.info("Mỗi bản ghi đã được gắn trace_id để theo dõi trong toàn bộ luồng xử lý")
            logger.info("Truy vấn Elasticsearch mẫu: trace_id:<uuid> để tìm một bản ghi cụ thể")
            logger.info("Truy vấn Elasticsearch mẫu: status:error AND trace_id:<uuid> để tìm lỗi cụ thể")
            logger.info("Truy vấn Elasticsearch mẫu: _id:<record_id> để tìm tất cả các bước của một bản ghi")
            
            # Tìm Producer nhanh nhất và chậm nhất
            producer_times = [(i, self.producer_end_times[i] - self.producer_start_times[i]) 
                            for i in range(len(self.producer_start_times))
                            if self.producer_start_times[i] > 0 and self.producer_end_times[i] > 0]
            
            if producer_times:
                fastest_idx, fastest_time = min(producer_times, key=lambda x: x[1])
                slowest_idx, slowest_time = max(producer_times, key=lambda x: x[1])
                
                logger.info(f"Producer nhanh nhất: #{fastest_idx} ({format_duration(fastest_time)})")
                logger.info(f"Producer chậm nhất: #{slowest_idx} ({format_duration(slowest_time)})")
            
            return result
                
        except Exception as e:
            logger.error(f"Lỗi khi thực hiện quy trình: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
        
        finally:
            # Dừng thread giám sát metrics
            self.stop_metrics_monitor()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="MongoDB to Kafka Monitoring Script")
    parser.add_argument("--config", help="Path to configuration file")
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Create and run the monitor
    monitor = MongoToKafkaMonitor(config)
    result = monitor.run()
    
    # Exit code based on result
    if result["status"] == "success":
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())