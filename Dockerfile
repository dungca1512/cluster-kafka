# Sử dụng image Python 3.13
FROM python:3.13-slim

# Đặt thư mục làm việc trong container
WORKDIR /app

# Sao chép tất cả các file vào container
COPY . .

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# Lệnh chạy khi container được start
CMD ["python", "mongo_to_kafka_monitor.py"]
