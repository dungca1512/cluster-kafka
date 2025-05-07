import json
import threading
import time
from datetime import datetime
import math
from loguru import logger
from confluent_kafka import Producer
from typing import List, Dict, Any, Callable, Optional

class KafkaProducer:
    """
    Class để gửi dữ liệu vào Kafka sử dụng nhiều producer chạy song song
    """
    def __init__(self, bootstrap_servers: str, client_id_prefix: str = "producer"):
        """
        Khởi tạo Kafka Producer
        
        Args:
            bootstrap_servers: Chuỗi kết nối đến Kafka brokers
            client_id_prefix: Tiền tố cho client ID của producer
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id_prefix = client_id_prefix
        logger.debug(f"Khởi tạo KafkaProducer với bootstrap_servers: {bootstrap_servers}")
    
    def create_producer(self, producer_id: int = 0) -> Producer:
        """
        Tạo một Producer mới
        
        Args:
            producer_id: ID của producer (cho logging và client_id)
            
        Returns:
            Producer: Instance của Kafka Producer
        """
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f"{self.client_id_prefix}-{producer_id}",
            'linger.ms': 5,  # Gộp message để hiệu quả hơn
            'batch.size': 16384,
            'queue.buffering.max.messages': 100000,
        }
        
        return Producer(conf)
    
    def delivery_report(self, err, msg):
        """
        Callback được gọi khi message được gửi đi hoặc gặp lỗi
        
        Args:
            err: Lỗi nếu có
            msg: Message đã gửi
        """
        if err is not None:
            logger.error(f"Lỗi gửi message: {err}")
        else:
            logger.debug(f"Message đã gửi đến {msg.topic()} [partition {msg.partition()}]")
    
    def send_message(self, topic: str, message: Any, producer: Optional[Producer] = None) -> bool:
        """
        Gửi một message vào Kafka topic
        
        Args:
            topic: Tên topic
            message: Nội dung message (sẽ được chuyển thành JSON nếu là dict hoặc list)
            producer: Producer instance (nếu None sẽ tạo mới)
            
        Returns:
            bool: True nếu gửi thành công, False nếu thất bại
        """
        close_producer = False
        
        try:
            # Tạo producer nếu không được cung cấp
            if producer is None:
                producer = self.create_producer()
                close_producer = True
            
            # Chuyển message thành JSON nếu là dict hoặc list
            if isinstance(message, (dict, list)):
                message_str = json.dumps(message)
            else:
                message_str = str(message)
            
            # Gửi message vào topic
            producer.produce(
                topic=topic,
                value=message_str.encode('utf-8'),
                callback=self.delivery_report
            )
            
            # Flush nếu tự tạo producer
            if close_producer:
                producer.flush(timeout=5)
            
            return True
        
        except Exception as e:
            logger.error(f"Lỗi khi gửi message vào topic {topic}: {str(e)}")
            return False
        
        finally:
            # Đóng producer nếu tự tạo
            if close_producer and producer is not None:
                try:
                    producer.flush()
                except Exception:
                    pass

    def send_messages_batch(self, topic: str, messages: List[Any]) -> int:
        """
        Gửi một batch các message vào Kafka topic
        
        Args:
            topic: Tên topic
            messages: Danh sách các message
            
        Returns:
            int: Số message đã gửi thành công
        """
        if not messages:
            return 0
        
        producer = self.create_producer()
        success_count = 0
        
        try:
            for idx, message in enumerate(messages):
                if self.send_message(topic, message, producer):
                    success_count += 1
                
                # Flush định kỳ để tránh quá tải buffer
                if (idx + 1) % 1000 == 0:
                    producer.flush()
            
            # Flush lần cuối
            producer.flush()
            
            return success_count
        
        except Exception as e:
            logger.error(f"Lỗi khi gửi batch messages: {str(e)}")
            return success_count
        
        finally:
            try:
                producer.flush()
            except Exception:
                pass
    
    def split_data(self, data: List[Any], num_parts: int) -> List[List[Any]]:
        """
        Chia danh sách dữ liệu thành nhiều phần để xử lý song song
        
        Args:
            data: Danh sách dữ liệu cần chia
            num_parts: Số phần cần chia
            
        Returns:
            List[List[Any]]: Danh sách các phần đã chia
        """
        if not data:
            return []
        
        if num_parts <= 1:
            return [data]
        
        # Điều chỉnh số phần nếu ít hơn dữ liệu
        num_parts = min(num_parts, len(data))
        
        # Chia dữ liệu
        chunk_size = math.ceil(len(data) / num_parts)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        logger.debug(f"Chia {len(data)} bản ghi thành {len(chunks)} phần")
        return chunks
    
    def producer_task(self, data: List[Any], topic: str, producer_id: int) -> Dict[str, Any]:
        """
        Hàm được chạy trong từng thread để gửi dữ liệu
        
        Args:
            data: Dữ liệu cần gửi
            topic: Topic Kafka
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
        
        start_time = time.time()
        producer = self.create_producer(producer_id)
        success_count = 0
        
        try:
            logger.info(f"Producer {producer_id} bắt đầu gửi {len(data)} bản ghi")
            
            for idx, message in enumerate(data):
                if self.send_message(topic, message, producer):
                    success_count += 1
                
                # Flush định kỳ
                if (idx + 1) % 1000 == 0:
                    producer.flush()
                    logger.debug(f"Producer {producer_id}: Đã gửi {idx + 1}/{len(data)} bản ghi")
            
            # Flush lần cuối
            producer.flush()
            
            elapsed_time = time.time() - start_time
            rate = len(data) / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(f"Producer {producer_id} hoàn thành: {success_count}/{len(data)} bản ghi "
                      f"trong {elapsed_time:.2f}s ({rate:.2f} bản ghi/giây)")
            
            return {
                "producer_id": producer_id,
                "success_count": success_count,
                "total_count": len(data),
                "elapsed_time": elapsed_time
            }
        
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Producer {producer_id} gặp lỗi: {str(e)}")
            
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
            except Exception:
                pass
    
    def send_data_parallel(self, topic: str, data: List[Any], num_producers: int = 10) -> Dict[str, Any]:
        """
        Gửi dữ liệu vào Kafka sử dụng nhiều producer chạy song song
        
        Args:
            topic: Topic Kafka
            data: Dữ liệu cần gửi
            num_producers: Số lượng producer (thread) chạy song song
            
        Returns:
            Dict[str, Any]: Kết quả gửi dữ liệu
        """
        if not data:
            logger.warning(f"Không có dữ liệu để gửi vào topic {topic}")
            return {
                "success_count": 0,
                "total_count": 0,
                "elapsed_time": 0
            }
        
        # Chia dữ liệu thành các phần
        data_chunks = self.split_data(data, num_producers)
        num_chunks = len(data_chunks)
        
        # Điều chỉnh số producer theo số lượng chunk
        num_producers = min(num_producers, num_chunks)
        
        start_time = time.time()
        threads = []
        results = []
        
        logger.info(f"Bắt đầu gửi {len(data)} bản ghi vào topic {topic} sử dụng {num_producers} producer")
        
        # Tạo và khởi động các thread
        for i in range(num_chunks):
            thread = threading.Thread(
                target=lambda idx=i, chunk=data_chunks[i]: 
                    results.append(self.producer_task(chunk, topic, idx))
            )
            threads.append(thread)
            thread.start()
        
        # Đợi tất cả thread hoàn thành
        for thread in threads:
            thread.join()
        
        # Tính toán kết quả
        elapsed_time = time.time() - start_time
        success_count = sum(r.get("success_count", 0) for r in results)
        total_count = sum(r.get("total_count", 0) for r in results)
        rate = total_count / elapsed_time if elapsed_time > 0 else 0
        
        logger.success(f"Hoàn thành gửi {success_count}/{total_count} bản ghi "
                      f"trong {elapsed_time:.2f}s ({rate:.2f} bản ghi/giây)")
        
        return {
            "success_count": success_count,
            "total_count": total_count,
            "elapsed_time": elapsed_time,
            "producer_results": results
        }


# Utility functions - Có thể gọi trực tiếp mà không cần tạo instance

def create_kafka_producer(bootstrap_servers: str, client_id: str = "simple-producer") -> Producer:
    """
    Tạo một Kafka producer đơn giản
    
    Args:
        bootstrap_servers: Địa chỉ các Kafka broker
        client_id: ID của client
        
    Returns:
        Producer: Instance của Kafka Producer
    """
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': client_id,
    }
    return Producer(conf)

def delivery_callback(err, msg):
    """
    Callback standard cho việc gửi message
    
    Args:
        err: Lỗi nếu có
        msg: Message đã gửi
    """
    if err is not None:
        logger.error(f"Lỗi gửi message: {err}")
    else:
        logger.debug(f"Message đã gửi đến {msg.topic()} [partition {msg.partition()}]")

def send_to_kafka(producer: Producer, topic: str, message: Any, 
                  callback: Optional[Callable] = None) -> bool:
    """
    Gửi một message vào Kafka
    
    Args:
        producer: Kafka Producer
        topic: Topic để gửi
        message: Nội dung cần gửi
        callback: Callback function (nếu None sẽ dùng mặc định)
        
    Returns:
        bool: True nếu gửi thành công, False nếu thất bại
    """
    try:
        # Chuẩn bị dữ liệu
        if isinstance(message, (dict, list)):
            message_str = json.dumps(message)
        else:
            message_str = str(message)
        
        # Sử dụng callback mặc định nếu không được cung cấp
        if callback is None:
            callback = delivery_callback
        
        # Gửi message
        producer.produce(
            topic=topic,
            value=message_str.encode('utf-8'),
            callback=callback
        )
        
        return True
    
    except Exception as e:
        logger.error(f"Lỗi khi gửi message: {str(e)}")
        return False

def send_messages_batch(producer: Producer, topic: str, messages: List[Any]) -> int:
    """
    Gửi một batch các message vào Kafka
    
    Args:
        producer: Kafka Producer
        topic: Topic để gửi
        messages: Danh sách các message
        
    Returns:
        int: Số message đã gửi thành công
    """
    success_count = 0
    
    try:
        for idx, message in enumerate(messages):
            if send_to_kafka(producer, topic, message):
                success_count += 1
            
            # Flush định kỳ
            if (idx + 1) % 1000 == 0:
                producer.flush()
        
        # Flush lần cuối
        producer.flush()
        
        return success_count
    
    except Exception as e:
        logger.error(f"Lỗi khi gửi batch messages: {str(e)}")
        return success_count

def send_data_parallel_simple(bootstrap_servers: str, topic: str, data: List[Any], 
                             num_producers: int = 5) -> Dict[str, Any]:
    """
    Hàm đơn giản để gửi dữ liệu song song mà không cần tạo instance
    
    Args:
        bootstrap_servers: Địa chỉ các Kafka broker
        topic: Topic để gửi dữ liệu
        data: Dữ liệu cần gửi
        num_producers: Số lượng producer
        
    Returns:
        Dict[str, Any]: Kết quả gửi dữ liệu
    """
    kafka_producer = KafkaProducer(bootstrap_servers)
    return kafka_producer.send_data_parallel(topic, data, num_producers)


# For testing
if __name__ == "__main__":
    # Cấu hình logging
    logger.remove()
    logger.add(
        lambda msg: print(msg),
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    
    # Test gửi dữ liệu
    bootstrap_servers = "localhost:9092"
    topic = "test"
    
    # Tạo dữ liệu test
    test_data = [{"id": i, "value": f"test-{i}"} for i in range(10)]
    
    # Test gửi đơn giản
    logger.info("=== Test gửi đơn giản ===")
    producer = create_kafka_producer(bootstrap_servers)
    for item in test_data[:3]:
        success = send_to_kafka(producer, topic, item)
        logger.info(f"Gửi {item}: {'Thành công' if success else 'Thất bại'}")
    producer.flush()
    
    # Test gửi batch
    logger.info("\n=== Test gửi batch ===")
    success_count = send_messages_batch(producer, topic, test_data[3:6])
    logger.info(f"Đã gửi {success_count}/3 messages")
    
    # Test gửi song song
    logger.info("\n=== Test gửi song song ===")
    result = send_data_parallel_simple(bootstrap_servers, topic, test_data[6:], 2)
    logger.info(f"Kết quả: {result['success_count']}/{result['total_count']} thành công")