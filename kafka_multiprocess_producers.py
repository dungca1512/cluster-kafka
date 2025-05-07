from confluent_kafka import Consumer, Producer, KafkaError, admin
import json
from multiprocessing import Pool
import math
from loguru import logger
import signal
import sys
import time

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Thay bằng địa chỉ Kafka của bạn
SOURCE_TOPIC = 'intermediate-notifications'  # Topic trung gian mà Kafka node gửi dữ liệu
DEST_TOPIC = 'notifications'  # Topic đích
GROUP_ID = 'n8n_event_processor'
NUM_PRODUCERS = 10
BATCH_SIZE = 500  # Giảm xuống 2.5 triệu để phù hợp với 2.6 triệu events
TIMEOUT_SECONDS = 10  # Thời gian chờ trước khi xử lý batch nếu không nhận thêm dữ liệu

def delivery_report(err, msg):
    """Callback để báo cáo kết quả gửi message"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")

def create_producer():
    """Tạo một Kafka Producer"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'n8n-event-producer',
        'acks': 'all',
        'retries': 5,
        'batch.size': 16384,
        'linger.ms': 5
    }
    return Producer(conf)

def send_events(producer_id, events):
    """Hàm gửi events tới Kafka từ một producer"""
    try:
        producer = create_producer()
        logger.info(f"Producer {producer_id} started with {len(events)} events")

        for idx, event in enumerate(events):
            try:
                event_data = json.dumps(event) if isinstance(event, dict) else str(event)
                producer.produce(
                    topic=DEST_TOPIC,
                    value=event_data.encode('utf-8'),
                    on_delivery=delivery_report
                )
                if (idx + 1) % 1000 == 0:
                    producer.flush()
                    logger.info(f"Producer {producer_id} flushed {idx + 1} messages")
            except Exception as e:
                logger.error(f"Producer {producer_id} failed to send event {idx}: {e}")

        producer.flush()
        logger.info(f"Producer {producer_id} completed")

    except Exception as e:
        logger.error(f"Producer {producer_id} encountered error: {e}")

def split_events(events, num_parts):
    """Chia danh sách events thành num_parts phần"""
    part_size = math.ceil(len(events) / num_parts)
    return [events[i:i + part_size] for i in range(0, len(events), part_size)]

def process_events(events):
    """Xử lý danh sách events với 10 producer"""
    try:
        if not events:
            logger.warning("No events to process.")
            return

        event_parts = split_events(events, NUM_PRODUCERS)
        logger.info(f"Split {len(events)} events into {len(event_parts)} parts")

        with Pool(processes=NUM_PRODUCERS) as pool:
            pool.starmap(send_events, [(i, part) for i, part in enumerate(event_parts)])

    except Exception as e:
        logger.error(f"Process events error: {e}")

def create_consumer():
    """Tạo một Kafka Consumer"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    return Consumer(conf)

def check_topics():
    """Kiểm tra sự tồn tại của topic nguồn và đích"""
    admin_client = admin.AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    topics = admin_client.list_topics(timeout=10).topics
    missing_topics = []
    
    if SOURCE_TOPIC not in topics:
        missing_topics.append(SOURCE_TOPIC)
    if DEST_TOPIC not in topics:
        missing_topics.append(DEST_TOPIC)
    
    if missing_topics:
        logger.error(f"Missing topics: {missing_topics}. Please create them using kafka-topics.sh.")
        sys.exit(1)
    logger.info(f"Topics {SOURCE_TOPIC} and {DEST_TOPIC} exist.")

def consumer_task():
    """Hàm chạy consumer, lắng nghe topic nguồn và kích hoạt xử lý"""
    try:
        consumer = create_consumer()
        consumer.subscribe([SOURCE_TOPIC])
        logger.info(f"Consumer started, listening to {SOURCE_TOPIC}")

        events = []
        last_message_time = time.time()

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # Nếu không nhận được message mới và đã quá thời gian chờ, xử lý batch hiện tại
                if events and (time.time() - last_message_time) > TIMEOUT_SECONDS:
                    logger.info(f"No new messages for {TIMEOUT_SECONDS} seconds, processing {len(events)} events...")
                    process_events(events)
                    consumer.commit(asynchronous=False)
                    logger.success(f"Processed {len(events)} events, resuming listening...")
                    events = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break

            try:
                # Cập nhật thời gian nhận message cuối cùng
                last_message_time = time.time()

                # Log dữ liệu thô để debug
                raw_data = msg.value().decode('utf-8')
                logger.debug(f"Raw message: {raw_data}")

                # Thử parse dữ liệu thành JSON
                try:
                    message_data = json.loads(raw_data)
                except json.JSONDecodeError:
                    # Nếu không phải JSON, xử lý như một event đơn
                    message_data = {"data": raw_data}

                # Nếu message_data là danh sách, thêm tất cả events vào
                if isinstance(message_data, list):
                    events.extend(message_data)
                    logger.info(f"Received {len(message_data)} events, total accumulated: {len(events)}")
                else:
                    events.append(message_data)
                    logger.info(f"Received 1 event, total accumulated: {len(events)}")

                # Kích hoạt xử lý nếu đạt đủ số lượng events
                if len(events) >= BATCH_SIZE:
                    logger.info(f"Reached {BATCH_SIZE} events, starting processing...")
                    process_events(events)
                    consumer.commit(asynchronous=False)
                    logger.success(f"Processed {len(events)} events, resuming listening...")
                    events = []

            except Exception as e:
                logger.error(f"Consumer failed to process message: {e}")

    except Exception as e:
        logger.error(f"Consumer encountered error: {e}")
    finally:
        consumer.close()

def signal_handler(sig, frame):
    """Xử lý tín hiệu dừng chương trình"""
    logger.info("Shutting down...")
    sys.exit(0)

def main():
    """Hàm chính để chạy consumer"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        check_topics()
        consumer_task()
    except Exception as e:
        logger.error(f"Main process error: {e}")

if __name__ == '__main__':
    main()