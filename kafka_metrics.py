import time
from prometheus_client import start_http_server, Counter, Gauge, Summary, Histogram
import os
import psutil
from typing import Dict, Any, Optional

class PrometheusMetrics:
    """
    Lớp quản lý metrics cho việc giám sát quá trình gửi dữ liệu từ MongoDB đến Kafka
    """
    def __init__(self, port: int = 8000):
        """
        Khởi tạo các metrics và mở HTTP server
        
        Args:
            port: Port để mở HTTP server
        """
        # MongoDB metrics
        self.mongodb_records_read = Counter(
            'mongodb_records_read_total', 
            'Tổng số bản ghi đã đọc từ MongoDB'
        )
        
        self.mongodb_read_time = Summary(
            'mongodb_read_time_seconds', 
            'Thời gian đọc dữ liệu từ MongoDB (seconds)'
        )
        
        self.mongodb_read_rate = Gauge(
            'mongodb_read_rate',
            'Tốc độ đọc dữ liệu từ MongoDB (records/second)'
        )
        
        self.mongodb_connection_errors = Counter(
            'mongodb_connection_errors_total', 
            'Tổng số lỗi kết nối MongoDB'
        )
        
        self.mongodb_query_errors = Counter(
            'mongodb_query_errors_total', 
            'Tổng số lỗi khi truy vấn MongoDB'
        )
        
        # Kafka metrics
        self.kafka_records_sent = Counter(
            'kafka_records_sent_total', 
            'Tổng số bản ghi đã gửi đến Kafka'
        )
        
        self.kafka_records_failed = Counter(
            'kafka_records_failed_total', 
            'Tổng số bản ghi gửi thất bại đến Kafka'
        )
        
        self.kafka_records_in_progress = Gauge(
            'kafka_records_in_progress',
            'Số bản ghi đang trong quá trình gửi đến Kafka'
        )
        
        self.kafka_send_time = Summary(
            'kafka_send_time_seconds', 
            'Thời gian gửi dữ liệu đến Kafka (seconds)'
        )
        
        self.kafka_batch_processing_time = Histogram(
            'kafka_batch_processing_time_seconds',
            'Thời gian xử lý batch Kafka (seconds)',
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]
        )
        
        self.kafka_producer_active = Gauge(
            'kafka_producer_active', 
            'Số producer Kafka đang hoạt động'
        )
        
        self.kafka_send_rate = Gauge(
            'kafka_send_rate',
            'Tốc độ gửi dữ liệu đến Kafka (records/second)'
        )
        
        self.kafka_success_rate = Gauge(
            'kafka_success_rate',
            'Tỷ lệ gửi thành công đến Kafka (%)'
        )
        
        self.kafka_connection_errors = Counter(
            'kafka_connection_errors_total', 
            'Tổng số lỗi kết nối Kafka'
        )
        
        # Per producer metrics
        self.producer_records_sent = {}
        self.producer_send_rate = {}
        
        # Application metrics
        self.app_total_processing_time = Summary(
            'app_total_processing_time_seconds', 
            'Tổng thời gian xử lý của ứng dụng (seconds)'
        )
        
        self.app_memory_usage = Gauge(
            'app_memory_usage_bytes', 
            'Bộ nhớ đang sử dụng của ứng dụng (bytes)'
        )
        
        self.app_cpu_usage = Gauge(
            'app_cpu_usage_percent',
            'Mức sử dụng CPU của ứng dụng (%)'
        )
        
        # Start HTTP server để expose metrics
        try:
            start_http_server(port)
            print(f"Prometheus metrics server đã khởi động tại port {port}")
        except Exception as e:
            print(f"Không thể khởi động Prometheus metrics server: {str(e)}")
    
    def register_producer(self, producer_id: int):
        """Đăng ký một producer để theo dõi metrics riêng"""
        if producer_id not in self.producer_records_sent:
            self.producer_records_sent[producer_id] = Counter(
                f'kafka_producer_{producer_id}_records_sent_total',
                f'Tổng số bản ghi đã gửi bởi producer {producer_id}'
            )
            
            self.producer_send_rate[producer_id] = Gauge(
                f'kafka_producer_{producer_id}_send_rate',
                f'Tốc độ gửi dữ liệu của producer {producer_id} (records/second)'
            )
    
    def track_memory_and_cpu(self):
        """Theo dõi bộ nhớ và CPU đang sử dụng của ứng dụng"""
        try:
            process = psutil.Process(os.getpid())
            
            # Theo dõi memory
            memory_usage = process.memory_info().rss  # resident set size
            self.app_memory_usage.set(memory_usage)
            
            # Theo dõi CPU
            cpu_percent = process.cpu_percent(interval=0.1)
            self.app_cpu_usage.set(cpu_percent)
            
        except Exception as e:
            print(f"Lỗi khi theo dõi tài nguyên: {str(e)}")
    
    def update_producer_metrics(self, producer_id: int, records_sent: int, elapsed_time: float):
        """Cập nhật metrics cho một producer cụ thể"""
        if producer_id not in self.producer_records_sent:
            self.register_producer(producer_id)
            
        # Tăng số lượng records đã gửi
        self.producer_records_sent[producer_id].inc(records_sent)
        
        # Cập nhật tốc độ gửi
        if elapsed_time > 0:
            send_rate = records_sent / elapsed_time
            self.producer_send_rate[producer_id].set(send_rate)
    
    def update_kafka_success_rate(self):
        """Cập nhật tỷ lệ gửi thành công đến Kafka"""
        total_sent = self.kafka_records_sent._value.get()
        total_failed = self.kafka_records_failed._value.get()
        total = total_sent + total_failed
        
        if total > 0:
            success_rate = (total_sent / total) * 100
            self.kafka_success_rate.set(success_rate)
    
    def begin_batch_processing(self, batch_size: int):
        """Đánh dấu bắt đầu xử lý một batch"""
        self.kafka_records_in_progress.inc(batch_size)
    
    def end_batch_processing(self, success_count: int, fail_count: int):
        """Đánh dấu kết thúc xử lý một batch"""
        total = success_count + fail_count
        self.kafka_records_in_progress.dec(total)
        self.kafka_records_sent.inc(success_count)
        self.kafka_records_failed.inc(fail_count)
        self.update_kafka_success_rate()


# Singleton instance để sử dụng trong toàn bộ ứng dụng
_metrics_instance = None

def setup_metrics(port: int = 8000) -> PrometheusMetrics:
    """
    Khởi tạo và cấu hình Prometheus metrics
    
    Args:
        port: Port để mở HTTP server
        
    Returns:
        PrometheusMetrics: Instance của PrometheusMetrics
    """
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = PrometheusMetrics(port=port)
    return _metrics_instance

def get_metrics() -> Optional[PrometheusMetrics]:
    """
    Lấy instance metrics đã khởi tạo
    
    Returns:
        PrometheusMetrics hoặc None nếu chưa khởi tạo
    """
    return _metrics_instance


if __name__ == "__main__":
    # Test Prometheus Metrics
    metrics = setup_metrics(port=8000)
    
    # Simulate some metrics
    start_time = time.time()
    
    # MongoDB metrics
    with metrics.mongodb_read_time.time():
        time.sleep(0.5)
        metrics.mongodb_records_read.inc(1000)
    
    elapsed = time.time() - start_time
    metrics.mongodb_read_rate.set(1000 / elapsed)
    
    # Producer metrics
    for i in range(3):
        metrics.register_producer(i)
        metrics.producer_records_sent[i].inc(100 * (i+1))
        metrics.producer_send_rate[i].set(10 * (i+1))
    
    # Kafka metrics
    metrics.kafka_producer_active.inc(3)
    metrics.kafka_records_sent.inc(250)
    metrics.kafka_records_failed.inc(50)
    metrics.update_kafka_success_rate()
    
    with metrics.kafka_batch_processing_time.time():
        time.sleep(1.0)
    
    # Track resources
    metrics.track_memory_and_cpu()
    
    print("Metrics server running on port 8000. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
            metrics.track_memory_and_cpu()
    except KeyboardInterrupt:
        print("Stopped")