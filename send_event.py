import json
import threading
import time
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, messaging
from confluent_kafka import Consumer, KafkaError
from loguru import logger
import os
import signal
import multiprocessing

# Configure loguru with colors
logger.remove()
logger.add(
    sink="kafka_fcm_consumer.log",
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
KAFKA_GROUP_ID = "fcm-consumer-group"
NUM_CONSUMERS = 10

# FCM configuration
FCM_CREDENTIAL_PATH = "path/to/your/firebase-credentials.json"  # Replace with your FCM credentials path
FCM_BATCH_SIZE = 500  # Maximum messages in a batch (FCM limit is 500)

# Shared counters for tracking stats
processed_messages = multiprocessing.Value('i', 0)
successful_fcm_sends = multiprocessing.Value('i', 0)
failed_fcm_sends = multiprocessing.Value('i', 0)
running = multiprocessing.Value('i', 1)  # 1 = running, 0 = shutdown    

# Handle termination signals
def handle_signal(sig, frame):
    logger.warning(f"Received signal {sig}, initiating graceful shutdown...")
    with running.get_lock():
        running.value = 0

# Register signal handlers
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

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

# Initialize Firebase Admin SDK
def initialize_firebase():
    try:
        if not os.path.exists(FCM_CREDENTIAL_PATH):
            logger.error(f"FCM credential file not found at: {FCM_CREDENTIAL_PATH}")
            return False
            
        cred = credentials.Certificate(FCM_CREDENTIAL_PATH)
        firebase_admin.initialize_app(cred)
        logger.success("Firebase Admin SDK initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Firebase Admin SDK: {e}")
        return False

# Send message to FCM
def send_to_fcm(tokens, message_data):
    if not tokens:
        return 0, 0  # No tokens to send to
    
    successful = 0
    failed = 0
    
    try:
        # Prepare multicast message
        multicast_message = messaging.MulticastMessage(
            tokens=tokens,
            data=message_data,
            android=messaging.AndroidConfig(
                priority="high",
                ttl=86400  # 24 hours in seconds
            ),
            apns=messaging.APNSConfig(
                headers={
                    "apns-priority": "10"
                },
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        content_available=True,
                    ),
                ),
            ),
        )
        
        # Send the message
        batch_response = messaging.send_multicast(multicast_message)
        
        # Process the response
        successful = batch_response.success_count
        failed = batch_response.failure_count
        
        # Log failures if any
        if failed > 0:
            responses = batch_response.responses
            for idx, resp in enumerate(responses):
                if not resp.success:
                    logger.warning(f"FCM send failed for token {tokens[idx][:10]}...: {resp.exception}")
        
        return successful, failed
    
    except Exception as e:
        logger.error(f"Error sending FCM message: {e}")
        return 0, len(tokens)  # Count all as failed

# Consumer function
def consumer_task(consumer_id, start_times, end_times):
    start_times[consumer_id] = time.time()
    last_log_time = time.time()
    message_count = 0
    
    # Configure Kafka consumer
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f"{KAFKA_GROUP_ID}-{consumer_id}",
        'auto.offset.reset': 'earliest',
        'session.timeout.ms': 60000,
        'max.poll.interval.ms': 300000,
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    
    logger.info(f"Consumer {consumer_id} started")
    
    try:
        # Initialize Firebase (one instance per consumer)
        if not initialize_firebase():
            logger.error(f"Consumer {consumer_id} exiting due to Firebase initialization failure")
            end_times[consumer_id] = time.time()
            return
        
        token_batch = []
        data_batch = {}
        
        while running.value == 1:
            # Poll for messages with a timeout
            msg = consumer.poll(1.0)
            
            if msg is None:
                # No message, check if we have a batch to send
                if token_batch:
                    success, failure = send_to_fcm(token_batch, data_batch)
                    
                    with successful_fcm_sends.get_lock(), failed_fcm_sends.get_lock():
                        successful_fcm_sends.value += success
                        failed_fcm_sends.value += failure
                    
                    token_batch = []
                    data_batch = {}
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Consumer {consumer_id} reached end of partition")
                else:
                    logger.error(f"Consumer {consumer_id} error: {msg.error()}")
                continue
            
            # Process the message
            try:
                # Parse the message value
                record = json.loads(msg.value().decode('utf-8'))
                
                # Extract token and data
                token = record.get('fcm_token')
                if not token:
                    logger.warning(f"Message has no FCM token: {record}")
                    continue
                
                # Prepare data for FCM message
                notification_data = {
                    "title": record.get("title", "Notification"),
                    "body": record.get("body", "You have a new notification"),
                    "timestamp": str(int(time.time())),
                    # Add any other fields from record that you want to send
                }
                
                # Add to batch
                token_batch.append(token)
                data_batch = notification_data  # Same data for all tokens in this batch
                
                # Send if batch is full
                if len(token_batch) >= FCM_BATCH_SIZE:
                    success, failure = send_to_fcm(token_batch, data_batch)
                    
                    with successful_fcm_sends.get_lock(), failed_fcm_sends.get_lock():
                        successful_fcm_sends.value += success
                        failed_fcm_sends.value += failure
                    
                    token_batch = []
                    data_batch = {}
                
                # Update message count
                message_count += 1
                with processed_messages.get_lock():
                    processed_messages.value += 1
                
                # Log progress periodically
                current_time = time.time()
                if current_time - last_log_time >= 10:  # Log every 10 seconds
                    elapsed = current_time - start_times[consumer_id]
                    rate = message_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Consumer {consumer_id} processed {message_count} messages ({rate:.2f} msg/sec)")
                    last_log_time = current_time
                
            except json.JSONDecodeError:
                logger.error(f"Consumer {consumer_id} received invalid JSON: {msg.value()}")
            except Exception as e:
                logger.error(f"Consumer {consumer_id} error processing message: {e}")
    
    except Exception as e:
        logger.error(f"Consumer {consumer_id} encountered an error: {e}")
    
    finally:
        # Send any remaining messages in the batch
        if token_batch:
            success, failure = send_to_fcm(token_batch, data_batch)
            
            with successful_fcm_sends.get_lock(), failed_fcm_sends.get_lock():
                successful_fcm_sends.value += success
                failed_fcm_sends.value += failure
        
        consumer.close()
        end_times[consumer_id] = time.time()
        elapsed = end_times[consumer_id] - start_times[consumer_id]
        rate = message_count / elapsed if elapsed > 0 else 0
        logger.success(f"Consumer {consumer_id} finished after processing {message_count} messages in {elapsed:.2f} seconds ({rate:.2f} msg/sec)")

# Stats reporter thread
def stats_reporter(start_times):
    global_start = min(t for t in start_times if t > 0) if any(t > 0 for t in start_times) else time.time()
    last_processed = 0
    last_time = time.time()
    
    while running.value == 1:
        time.sleep(5)  # Report every 5 seconds
        
        current_time = time.time()
        interval = current_time - last_time
        
        with processed_messages.get_lock(), successful_fcm_sends.get_lock(), failed_fcm_sends.get_lock():
            current_processed = processed_messages.value
            current_success = successful_fcm_sends.value
            current_failed = failed_fcm_sends.value
            
            new_messages = current_processed - last_processed
            rate = new_messages / interval if interval > 0 else 0
            total_elapsed = current_time - global_start
            avg_rate = current_processed / total_elapsed if total_elapsed > 0 else 0
            
            success_rate = (current_success / current_processed) * 100 if current_processed > 0 else 0
            
            logger.info(f"Stats: Processed {current_processed} msgs (Rate: {rate:.2f} msg/s, Avg: {avg_rate:.2f} msg/s)")
            logger.info(f"FCM: Success: {current_success} ({success_rate:.2f}%), Failed: {current_failed}")
            
            last_processed = current_processed
            last_time = current_time

# Main function
def main():
    logger.info(f"Starting Kafka to FCM relay with {NUM_CONSUMERS} consumers")
    logger.info(f"Process started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Arrays to track timing for each consumer
    start_times = [0] * NUM_CONSUMERS
    end_times = [0] * NUM_CONSUMERS
    
    # Start stats reporter in a separate thread
    stats_thread = threading.Thread(target=stats_reporter, args=(start_times,))
    stats_thread.daemon = True
    stats_thread.start()
    
    # Start consumer processes
    processes = []
    for i in range(NUM_CONSUMERS):
        process = multiprocessing.Process(target=consumer_task, args=(i, start_times, end_times))
        processes.append(process)
        process.start()
    
    # Wait for all processes to complete
    for process in processes:
        process.join()
    
    # Calculate total stats
    global_end_time = time.time()
    valid_start_times = [t for t in start_times if t > 0]
    global_start_time = min(valid_start_times) if valid_start_times else global_end_time
    
    global_elapsed = global_end_time - global_start_time
    
    # Find slowest and fastest consumer
    valid_times = [(i, end_times[i] - start_times[i]) for i in range(NUM_CONSUMERS) 
                  if end_times[i] > 0 and start_times[i] > 0]
    
    if valid_times:
        slowest_consumer, slowest_time = max(valid_times, key=lambda x: x[1])
        fastest_consumer, fastest_time = min(valid_times, key=lambda x: x[1])
    else:
        slowest_consumer, slowest_time = -1, 0
        fastest_consumer, fastest_time = -1, 0
    
    # Log summary
    with processed_messages.get_lock(), successful_fcm_sends.get_lock(), failed_fcm_sends.get_lock():
        logger.success("All consumers have completed")
        logger.info("=== Performance Summary ===")
        logger.info(f"Total time: {format_duration(global_elapsed)} ({global_elapsed:.2f} seconds)")
        logger.info(f"Total messages processed: {processed_messages.value}")
        
        avg_rate = processed_messages.value / global_elapsed if global_elapsed > 0 else 0
        logger.info(f"Average processing rate: {avg_rate:.2f} messages/second")
        
        logger.info(f"FCM success: {successful_fcm_sends.value}")
        logger.info(f"FCM failures: {failed_fcm_sends.value}")
        
        success_rate = (successful_fcm_sends.value / processed_messages.value) * 100 if processed_messages.value > 0 else 0
        logger.info(f"FCM success rate: {success_rate:.2f}%")
    
    if slowest_time > 0:
        logger.info(f"Slowest consumer: #{slowest_consumer} ({format_duration(slowest_time)})")
    if fastest_time > 0:
        logger.info(f"Fastest consumer: #{fastest_consumer} ({format_duration(fastest_time)})")
    
    logger.info(f"Process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()