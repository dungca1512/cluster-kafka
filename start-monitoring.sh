#!/bin/bash

# Script để khởi động hệ thống giám sát MongoDB to Kafka

# Kiểm tra các thư mục cần thiết
mkdir -p prometheus
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/provisioning/dashboards

# Kiểm tra file cấu hình đã tồn tại
if [ ! -f "prometheus/prometheus.yml" ]; then
    echo "Không tìm thấy file prometheus.yml. Vui lòng tạo file này trước khi tiếp tục."
    exit 1
fi

if [ ! -f "grafana/provisioning/datasources/datasource.yml" ]; then
    echo "Không tìm thấy file datasource.yml. Vui lòng tạo file này trước khi tiếp tục."
    exit 1
fi

if [ ! -f "grafana/provisioning/dashboards/dashboards.yml" ]; then
    echo "Không tìm thấy file dashboards.yml. Vui lòng tạo file này trước khi tiếp tục."
    exit 1
fi

if [ ! -f "grafana/provisioning/dashboards/mongodb_kafka_dashboard.json" ]; then
    echo "Không tìm thấy file mongodb_kafka_dashboard.json. Vui lòng tạo file này trước khi tiếp tục."
    exit 1
fi

# Kiểm tra docker-compose đã cài đặt
if ! command -v docker-compose &> /dev/null; then
    echo "docker-compose không được tìm thấy. Vui lòng cài đặt docker-compose trước khi tiếp tục."
    exit 1
fi

# Khởi động các container Docker
echo "Khởi động các container Docker..."
docker-compose up -d

# Đợi các dịch vụ khởi động
echo "Đợi các dịch vụ khởi động..."
sleep 10

# Kiểm tra kết nối đến Prometheus
echo "Kiểm tra kết nối đến Prometheus..."
curl -s http://localhost:9090/-/healthy > /dev/null
if [ $? -ne 0 ]; then
    echo "Không thể kết nối đến Prometheus. Vui lòng kiểm tra logs của container."
    echo "docker logs prometheus"
    exit 1
else
    echo "Prometheus đang chạy."
fi

# Kiểm tra kết nối đến Grafana
echo "Kiểm tra kết nối đến Grafana..."
curl -s http://localhost:3000/api/health > /dev/null
if [ $? -ne 0 ]; then
    echo "Không thể kết nối đến Grafana. Vui lòng kiểm tra logs của container."
    echo "docker logs grafana"
    exit 1
else
    echo "Grafana đang chạy."
fi

# Kiểm tra kết nối đến MongoDB
echo "Kiểm tra kết nối đến MongoDB..."
docker exec -it mongodb mongosh --eval "db.adminCommand('ping')" > /dev/null
if [ $? -ne 0 ]; then
    echo "Không thể kết nối đến MongoDB. Vui lòng kiểm tra logs của container."
    echo "docker logs mongodb"
    exit 1
else
    echo "MongoDB đang chạy."
fi

# Kiểm tra script giám sát
echo "Kiểm tra script giám sát..."
if [ ! -f "mongo_to_kafka_monitor.py" ]; then
    echo "Không tìm thấy file mongo_to_kafka_monitor.py. Vui lòng tạo file này trước khi tiếp tục."
    exit 1
fi

if [ ! -f "prometheus_metrics.py" ]; then
    echo "Không tìm thấy file prometheus_metrics.py. Vui lòng tạo file này trước khi tiếp tục."
    exit 1
fi

if [ ! -f "config.ini" ]; then
    echo "Không tìm thấy file config.ini. Vui lòng tạo file này trước khi tiếp tục."
    exit 1
fi

# Khởi động script giám sát
echo "Khởi động script giám sát..."
python mongo_to_kafka_monitor.py --config config.ini &

# In thông tin truy cập
echo ""
echo "Hệ thống giám sát đã khởi động thành công!"
echo ""
echo "Bạn có thể truy cập các dịch vụ sau:"
echo "- Kafka UI: http://localhost:8080"
echo "- Prometheus: http://localhost:9090"
echo "- Grafana: http://localhost:3000 (admin/admin)"
echo "- n8n: http://localhost:5678 (admin/matkhau)"
echo "- MongoDB: localhost:27017"
echo ""
echo "Script giám sát đang chạy ở background. Để xem logs:"
echo "ps aux | grep mongo_to_kafka_monitor.py"
echo ""
echo "Chúc bạn có trải nghiệm giám sát tốt!"