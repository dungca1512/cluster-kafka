# MongoDB to Kafka Monitoring Stack

Hệ thống giám sát quá trình lấy dữ liệu từ MongoDB và đẩy lên Kafka sử dụng Prometheus và Grafana.

## Cấu trúc thư mục

Trước khi khởi động, đảm bảo bạn có cấu trúc thư mục như sau:

```
/
├── docker-compose.yml
├── prometheus/
│   └── prometheus.yml
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── datasource.yml
│       └── dashboards/
│           ├── dashboards.yml
│           └── mongodb_kafka_dashboard.json
├── mongo_to_kafka_monitor.py
└── prometheus_metrics.py
```

## Chuẩn bị

1. Tạo các thư mục cần thiết:

```bash
mkdir -p prometheus
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/provisioning/dashboards
```

2. Sao chép các file cấu hình vào thư mục tương ứng.

3. Đảm bảo file `mongodb_kafka_dashboard.json` đã được đặt vào thư mục `grafana/provisioning/dashboards/`.

## Khởi động hệ thống

```bash
docker-compose up -d
```

## Truy cập các dịch vụ

- **Kafka UI**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **n8n**: http://localhost:5678 (admin/matkhau)
- **MongoDB**: localhost:27017

## Chạy script giám sát

Script `mongo_to_kafka_monitor.py` cần được chạy trên máy host (không phải trong container) để có thể truy cập cả MongoDB và Kafka.

```bash
python mongo_to_kafka_monitor.py --config config.ini
```

Script sẽ lấy dữ liệu từ MongoDB, gửi lên Kafka và cung cấp metrics tại địa chỉ http://localhost:8000/metrics, được Prometheus trong container thu thập và hiển thị trên Grafana.

## Cấu hình để kết nối từ container đến host

Trong file `prometheus.yml`, chúng tôi sử dụng `host.docker.internal:8000` để Prometheus trong container có thể truy cập vào metrics API của script chạy trên host. Đảm bảo máy host của bạn cho phép kết nối đến port 8000.

## Lưu ý

- Khi script `mongo_to_kafka_monitor.py` chạy lần đầu, dashboard Grafana có thể không hiển thị dữ liệu ngay lập tức. Chờ một khoảng thời gian để Prometheus thu thập đủ dữ liệu.
- Nếu không thấy metrics trên Grafana, kiểm tra:
  1. Script giám sát đang chạy và cung cấp metrics tại http://localhost:8000/metrics
  2. Prometheus có thể kết nối đến host.docker.internal:8000
  3. Grafana có thể kết nối đến Prometheus