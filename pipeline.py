import sys
import json
import re
from loguru import logger
from dotenv import load_dotenv
import os

# Import các module đã tạo
from api_receiver import ApiReceiver
from push_event import KafkaProducer
from merge_data import MergeData

# Cấu hình logging
logger.remove()
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO"
)
logger.add(
    "pipeline.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    level="INFO"
)

# Tải biến môi trường từ file .env
load_dotenv()


# Cấu hình
API_HOST = os.getenv("API_HOST")
API_PORT = int(os.getenv("API_PORT", 9000))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
NUM_PRODUCERS = 5

# Khởi tạo các đối tượng từ module
kafka_producer = KafkaProducer(KAFKA_BOOTSTRAP_SERVERS, client_id_prefix="pipeline")
merger = MergeData(MONGO_URI, MONGO_DB, MONGO_COLLECTION)
api_receiver = ApiReceiver(API_HOST, API_PORT)

def extract_json_from_string(input_string):
    """Trích xuất JSON từ string"""
    try:
        # Xử lý các trường hợp đầu vào khác nhau
        if isinstance(input_string, dict) or isinstance(input_string, list):
            return input_string
            
        # Xử lý trường hợp chuỗi có tiền tố 'return'
        if isinstance(input_string, str) and input_string.strip().startswith('return '):
            json_string = input_string.replace('return ', '', 1)
        else:
            json_string = input_string
        
        # Xử lý các ký tự escape
        if isinstance(json_string, str):
            # Thay thế các backslash đơn thành backslash kép (trừ những backslash đã escape \n)
            fixed_json_string = re.sub(r'\\(?!n)', r'\\\\', json_string)
            
            try:
                return json.loads(fixed_json_string)
            except json.JSONDecodeError as e:
                logger.error(f"Lỗi phân tích JSON: {e}")
                raise
        
        return json_string
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất JSON: {str(e)}")
        raise

def process_n8n_data(data):
    """Xử lý dữ liệu từ n8n"""
    try:
        # Trích xuất trường data nếu có
        if isinstance(data, dict) and 'data' in data:
            raw_data = data['data']
            logger.debug(f"Đang xử lý dữ liệu từ trường 'data'")
        else:
            raw_data = data
            logger.debug("Đang xử lý dữ liệu trực tiếp")
        
        # Parse JSON
        parsed_data = extract_json_from_string(raw_data)
        
        # Đảm bảo kết quả là danh sách
        if not isinstance(parsed_data, list):
            parsed_data = [parsed_data] if parsed_data else []
        
        logger.info(f"Đã parse thành công {len(parsed_data)} thông báo")
        return parsed_data
    
    except Exception as e:
        logger.error(f"Lỗi khi xử lý dữ liệu n8n: {str(e)}")
        raise

def handle_n8n_data(data):
    """
    Hàm xử lý dữ liệu từ n8n
    
    Args:
        data: Dữ liệu từ n8n
        
    Returns:
        dict: Kết quả xử lý
    """
    try:
        # 1. Parse thông báo từ dữ liệu n8n
        notifications = process_n8n_data(data)
        
        if not notifications:
            logger.warning("Không có thông báo nào để xử lý")
            return {
                "status": "warning",
                "message": "Không có thông báo nào để xử lý"
            }
        
        logger.info(f"Bắt đầu quá trình xử lý {len(notifications)} thông báo")
        
        # 2. Merge thông báo với device tokens từ MongoDB
        merged_events = merger.merge_notifications_with_devices(notifications)
        
        if not merged_events:
            logger.warning("Không có sự kiện nào sau khi merge với device tokens")
            return {
                "status": "warning",
                "message": "Không có sự kiện nào sau khi merge với device tokens",
                "notifications_count": len(notifications)
            }
        
        logger.info(f"Đã tạo {len(merged_events)} sự kiện, bắt đầu gửi vào Kafka")
        
        # 3. Gửi vào Kafka sử dụng module push_event
        kafka_result = kafka_producer.send_data_parallel(
            topic=KAFKA_TOPIC,
            data=merged_events,
            num_producers=NUM_PRODUCERS
        )
        
        success_rate = (kafka_result['success_count'] / kafka_result['total_count']) * 100 if kafka_result['total_count'] > 0 else 0
        
        # 4. Trả về kết quả chi tiết
        return {
            "status": "success" if kafka_result['success_count'] == kafka_result['total_count'] else "partial_success",
            "notifications": len(notifications),
            "device_events": kafka_result['total_count'],
            "sent_to_kafka": kafka_result['success_count'],
            "success_rate": f"{success_rate:.2f}%",
            "elapsed_time": f"{kafka_result['elapsed_time']:.2f} giây",
            "message": f"Đã xử lý {len(notifications)} thông báo, tạo {kafka_result['total_count']} sự kiện và gửi thành công {kafka_result['success_count']} vào Kafka"
        }
    
    except Exception as e:
        logger.error(f"Lỗi khi xử lý dữ liệu: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "message": "Có lỗi xảy ra khi xử lý dữ liệu"
        }

def main():
    """Hàm chính khởi động pipeline"""
    try:
        logger.info("=== KHỞI ĐỘNG PIPELINE ===")
        logger.info(f"API server sẽ chạy tại: http://{API_HOST}:{API_PORT}")
        logger.info(f"Kafka topic đang sử dụng: {KAFKA_TOPIC}")
        logger.info(f"MongoDB: {MONGO_URI}/{MONGO_DB}.{MONGO_COLLECTION}")
        logger.info(f"Số lượng producer song song: {NUM_PRODUCERS}")
        
        # Kiểm tra kết nối MongoDB
        if merger.connect_to_mongodb():
            logger.success("Kết nối MongoDB thành công")
            merger.close_connection()
        else:
            logger.warning("Không thể kết nối đến MongoDB, sẽ sử dụng dữ liệu mẫu nếu cần")
        
        # Đăng ký handler xử lý dữ liệu với API receiver
        api_receiver.register_handler(handle_n8n_data)
        
        # Khởi động API server
        api_receiver.start()
        
    except Exception as e:
        logger.error(f"Lỗi khởi động pipeline: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)