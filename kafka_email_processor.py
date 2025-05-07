# kafka_email_processor.py
import json
import threading
import os
import time
import sys
from typing import Dict, List, Optional
from confluent_kafka import Consumer, KafkaError
from email_sender import EmailSender
from dotenv import load_dotenv
from loguru import logger

# Cấu hình loguru
logger.remove()  # Xóa handler mặc định
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    "kafka_processor.log",
    rotation="10 MB",
    retention="1 week",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO"
)

# Load biến môi trường
load_dotenv()

class KafkaProcessor:
    def __init__(self, kafka_config=None, email_config=None):
        """
        Khởi tạo KafkaProcessor với cấu hình
        
        Args:
            kafka_config (dict, optional): Cấu hình Kafka
            email_config (dict, optional): Cấu hình email
        """
        # Cấu hình Kafka
        if kafka_config is None:
            self.kafka_config = {
                'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:29092,kafka2:29093,kafka3:29094'),
                'group.id': os.getenv('KAFKA_GROUP_ID', 'email-notification-processor'),
                'auto.offset.reset': 'latest'
            }
        else:
            self.kafka_config = kafka_config
            
        # Khởi tạo email sender
        self.email_sender = EmailSender(email_config)
        
        # Tên các topic
        self.segment_topic = os.getenv('KAFKA_SEGMENT_TOPIC', 'segment-topic')
        self.notification_topic = os.getenv('KAFKA_NOTIFICATION_TOPIC', 'notification-topic')
        
        # Lưu trữ dữ liệu
        self.segments_data: Dict[str, List[str]] = {}  # {segment_name: [email1, email2, ...]}
        self.notifications_data: Dict[str, Dict] = {}  # {segment_name: {country_code: notification_text}}
        
        # Lock để đồng bộ hóa giữa các threads
        self.lock = threading.Lock()
        
        # Biến điều khiển
        self.running = False
        
        logger.info("Kafka processor initialized")
    
    def process_segment_message(self, message):
        """
        Xử lý message từ topic segment và lưu thông tin email
        
        Args:
            message (str): Message JSON từ Kafka
        """
        try:
            data = json.loads(message)
            segment_name = data.get('segment')
            emails = data.get('emails', [])
            
            if not segment_name:
                logger.warning(f"Invalid segment message (missing segment name): {message}")
                return
            
            with self.lock:
                # Lưu vào danh sách segments ngay cả khi emails rỗng
                self.segments_data[segment_name] = emails
                
                if not emails:
                    logger.info(f"Segment {segment_name} has no emails to notify")
                else:
                    logger.info(f"Processed segment data for: {segment_name} with {len(emails)} emails")
                
                # Kiểm tra nếu đã có notification cho segment này và có email
                if segment_name in self.notifications_data and emails:
                    self._send_notifications(segment_name)
        
        except Exception as e:
            logger.error(f"Error processing segment message: {str(e)}")
            logger.exception("Stack trace:")
    
    def process_notification_message(self, message):
        """
        Xử lý message từ topic notification và gửi email nếu có thông tin segment tương ứng
        
        Args:
            message (str): Message JSON từ Kafka
        """
        try:
            data = json.loads(message)
            segment_name = data.get('segment')
            country_code = data.get('quốc gia')
            notification = data.get('notification')
            
            if not segment_name:
                logger.warning(f"Invalid notification message (missing segment): {message}")
                return
                
            if not notification:
                logger.warning(f"Invalid notification message (missing content): {message}")
                return
                
            if not country_code:
                logger.warning(f"Notification missing country code, using 'en' as default")
                country_code = "en"
            
            with self.lock:
                # Lưu thông báo theo segment và country_code
                if segment_name not in self.notifications_data:
                    self.notifications_data[segment_name] = {}
                
                self.notifications_data[segment_name][country_code] = notification
                logger.info(f"Processed notification for segment: {segment_name}, country: {country_code}")
                
                # Kiểm tra nếu đã có thông tin email cho segment này và có email
                if segment_name in self.segments_data and self.segments_data[segment_name]:
                    self._send_notifications(segment_name)
        
        except Exception as e:
            logger.error(f"Error processing notification message: {str(e)}")
            logger.exception("Stack trace:")
    
    def _send_notifications(self, segment_name):
        """
        Gửi thông báo đến tất cả email trong segment
        
        Args:
            segment_name (str): Tên segment cần gửi thông báo
        """
        emails = self.segments_data.get(segment_name, [])
        notifications = self.notifications_data.get(segment_name, {})
        
        if not emails:
            logger.warning(f"No emails to notify for segment: {segment_name}")
            return
            
        if not notifications:
            logger.warning(f"No notification content for segment: {segment_name}")
            return
        
        logger.info(f"Sending notifications to {len(emails)} users in segment {segment_name}")
        
        # Ưu tiên sử dụng ngôn ngữ tiếng Anh, nếu không có thì dùng bất kỳ ngôn ngữ nào có sẵn
        notification_text = notifications.get('en', next(iter(notifications.values())))
        
        # Định dạng thông báo
        title, body = self.email_sender.format_notification(notification_text)
        subject = title or f"Notification for {segment_name}"
        
        # Gửi cho tất cả email trong segment
        sent_count = 0
        for email in emails:
            if self.email_sender.send_email(email, subject, body):
                sent_count += 1
        
        logger.success(f"Sent {sent_count}/{len(emails)} emails for segment: {segment_name}")
        
        # Xóa dữ liệu đã xử lý để tránh gửi lại
        del self.notifications_data[segment_name]
        del self.segments_data[segment_name]
        
        logger.info(f"Completed sending notifications for segment: {segment_name}")
    
    def start_segment_consumer(self):
        """Khởi động consumer lắng nghe topic segment"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries and self.running:
            try:
                consumer = Consumer(self.kafka_config)
                consumer.subscribe([self.segment_topic])
                
                logger.info(f"Started segment consumer for topic: {self.segment_topic}")
                
                while self.running:
                    try:
                        msg = consumer.poll(1.0)
                        if msg is None:
                            continue
                            
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                logger.debug(f"Reached end of partition")
                            else:
                                logger.error(f"Error: {msg.error()}")
                        else:
                            self.process_segment_message(msg.value().decode('utf-8'))
                    except Exception as e:
                        logger.error(f"Error processing segment message: {str(e)}")
                        time.sleep(1)  # Tránh vòng lặp quá nhanh
                
                consumer.close()
                return
                
            except Exception as e:
                logger.error(f"Error connecting to Kafka for segment topic: {str(e)}")
                retry_count += 1
                
                if retry_count < max_retries:
                    wait_time = 5 * retry_count  # Tăng thời gian chờ sau mỗi lần thử
                    logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached. Giving up connecting to Kafka for segment topic.")
                    break
    
    def start_notification_consumer(self):
        """Khởi động consumer lắng nghe topic notification"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries and self.running:
            try:
                consumer = Consumer(self.kafka_config)
                consumer.subscribe([self.notification_topic])
                
                logger.info(f"Started notification consumer for topic: {self.notification_topic}")
                
                while self.running:
                    try:
                        msg = consumer.poll(1.0)
                        if msg is None:
                            continue
                            
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                logger.debug(f"Reached end of partition")
                            else:
                                logger.error(f"Error: {msg.error()}")
                        else:
                            self.process_notification_message(msg.value().decode('utf-8'))
                    except Exception as e:
                        logger.error(f"Error processing notification message: {str(e)}")
                        time.sleep(1)  # Tránh vòng lặp quá nhanh
                
                consumer.close()
                return
                
            except Exception as e:
                logger.error(f"Error connecting to Kafka for notification topic: {str(e)}")
                retry_count += 1
                
                if retry_count < max_retries:
                    wait_time = 5 * retry_count  # Tăng thời gian chờ sau mỗi lần thử
                    logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached. Giving up connecting to Kafka for notification topic.")
                    break
    
    def start(self):
        """Khởi động cả hai consumer trong hai thread riêng biệt"""
        self.running = True
        
        segment_thread = threading.Thread(target=self.start_segment_consumer)
        notification_thread = threading.Thread(target=self.start_notification_consumer)
        
        segment_thread.daemon = True
        notification_thread.daemon = True
        
        segment_thread.start()
        notification_thread.start()
        
        logger.success("Kafka processor started successfully")
        
        try:
            # Giữ chương trình chạy liên tục
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down Kafka processor...")
            self.running = False
            
        # Đợi các thread kết thúc
        segment_thread.join(timeout=5)
        notification_thread.join(timeout=5)
        
        logger.info("Kafka processor stopped")
    
    def stop(self):
        """Dừng processor"""
        self.running = False
        logger.info("Stop signal sent to Kafka processor")


if __name__ == "__main__":
    try:
        logger.info("Starting Kafka Email Processor")
        processor = KafkaProcessor()
        processor.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.exception("Stack trace:")