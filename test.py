import json
import threading
from confluent_kafka import Producer
import time
from loguru import logger
from pymongo import MongoClient
from datetime import datetime, timedelta

# Configure loguru with colors
logger.remove()
logger.add(
    sink="kafka_producer.log",
    rotation="10 MB",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO"
)
logger.add(
    sink=lambda msg: print(msg, end=""),
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO"
)

# Kafka configuration
KAFKA_TOPIC = "test"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
NUM_PRODUCERS = 10

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "user_fcm"  # Thay bằng tên database của bạn
MONGO_COLLECTION = "users_fcm"  # Thay bằng tên collection của bạn

# Load data from MongoDB
def load_from_mongo():
    start_time = time.time()
    logger.info(f"Connecting to MongoDB: {MONGO_URI}")
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Use cursor to stream data
        cursor = collection.find().batch_size(1000)  # Batch size để tối ưu
        total_records = collection.count_documents({})
        logger.success(f"Found {total_records} records in collection {MONGO_COLLECTION}")
        
        data = []
        for record in cursor:
            # Xóa trường _id (nếu cần) vì nó không serialize được trực tiếp thành JSON
            if "_id" in record:
                record["_id"] = str(record["_id"])
            data.append(record)
        
        client.close()
        elapsed_time = time.time() - start_time
        logger.info(f"MongoDB data loaded in {elapsed_time:.2f} seconds")
        return data
    except Exception as e:
        logger.error(f"Failed to load data from MongoDB: {e}")
        raise

# Callback to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Producer function
def producer_task(records, producer_id, start_times, end_times):
    start_times[producer_id] = time.time()
    
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": f"producer-{producer_id}",
        "queue.buffering.max.messages": 100000,
        "batch.num.messages": 5000,
        "linger.ms": 5,
    }
    producer = Producer(conf)
    
    logger.info(f"Producer {producer_id} started with {len(records)} records")
    
    for i, record in enumerate(records):
        try:
            record_json = json.dumps(record)
            producer.produce(
                KAFKA_TOPIC,
                value=record_json.encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            if (i + 1) % 5000 == 0:
                elapsed = time.time() - start_times[producer_id]
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                logger.info(f"Producer {producer_id} has sent {i + 1}/{len(records)} records (Rate: {rate:.2f} records/sec)")
            time.sleep(0.001)
        except Exception as e:
            logger.error(f"Producer {producer_id} failed to send record: {e}")
    
    producer.flush()
    end_times[producer_id] = time.time()
    elapsed = end_times[producer_id] - start_times[producer_id]
    rate = len(records) / elapsed if elapsed > 0 else 0
    logger.success(f"Producer {producer_id} finished sending {len(records)} records in {elapsed:.2f} seconds (Rate: {rate:.2f} records/sec)")

# Split data into chunks for each producer
def split_data(data, num_chunks):
    chunk_size = len(data) // num_chunks
    chunks = [data[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]
    if len(data) % num_chunks != 0:
        chunks[-1].extend(data[num_chunks * chunk_size:])
    logger.info(f"Data split into {len(chunks)} chunks")
    return chunks

# Format time duration nicely
def format_duration(seconds):
    duration = timedelta(seconds=seconds)
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

# Main function
def main():
    global_start_time = time.time()
    logger.info(f"Process started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data from MongoDB
    data = load_from_mongo()
    
    # Split data for producers
    chunks = split_data(data, NUM_PRODUCERS)
    
    # Arrays to track timing for each producer
    start_times = [0] * NUM_PRODUCERS
    end_times = [0] * NUM_PRODUCERS
    
    # Start producer threads
    threads = []
    for i in range(NUM_PRODUCERS):
        thread = threading.Thread(target=producer_task, args=(chunks[i], i, start_times, end_times))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    global_end_time = time.time()
    global_elapsed = global_end_time - global_start_time
    
    # Calculate statistics
    total_records = sum(len(chunk) for chunk in chunks)
    avg_rate = total_records / global_elapsed if global_elapsed > 0 else 0
    
    # Find slowest and fastest producer
    valid_times = [(i, end_times[i] - start_times[i]) for i in range(NUM_PRODUCERS) if end_times[i] > 0]
    if valid_times:
        slowest_producer, slowest_time = max(valid_times, key=lambda x: x[1])
        fastest_producer, fastest_time = min(valid_times, key=lambda x: x[1])
    else:
        slowest_producer, slowest_time = -1, 0
        fastest_producer, fastest_time = -1, 0
    
    # Log summary
    logger.success("All producers finished sending data to Kafka")
    logger.info("=== Performance Summary ===")
    logger.info(f"Total time: {format_duration(global_elapsed)} ({global_elapsed:.2f} seconds)")
    logger.info(f"Total records: {total_records}")
    logger.info(f"Average rate: {avg_rate:.2f} records/second")
    
    if slowest_time > 0:
        logger.info(f"Slowest producer: #{slowest_producer} ({format_duration(slowest_time)})")
    if fastest_time > 0:
        logger.info(f"Fastest producer: #{fastest_producer} ({format_duration(fastest_time)})")
    
    logger.info(f"Process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()