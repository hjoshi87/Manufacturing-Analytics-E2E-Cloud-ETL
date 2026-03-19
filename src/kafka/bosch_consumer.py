"""
Kafka Consumer to verify Bosch data stream
"""
from kafka import KafkaConsumer
import json
import logging
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY, KAFKA_MAX_MESSAGE_BYTES

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_messages(topic='machine_telemetry', max_messages=5):
    """Read and display messages from Kafka topic"""
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        # IMPORTANT: Increase fetch size to handle the large vibration arrays
        fetch_max_bytes=KAFKA_MAX_MESSAGE_BYTES,          # 5MB
        max_partition_fetch_bytes=5242880 # 5MB
    )
    
    logger.info(f"Listening to topic: {topic}")
    
    messages_received = 0
    messages_processed = 0 # Count only successful ones
    total_seen = 0         # Count every message in the topic
    
    try:
        for message in consumer:
            total_seen += 1
            msg_content = message.value
            
            # 1. Get the payload (handles both old and new formats)
            payload = msg_content.get('payload') or msg_content.get('data')
            
            if not payload or not isinstance(payload, dict):
                logger.warning(f"Message {total_seen} has unknown format. Skipping.")
                continue

            messages_processed += 1
            
            # 2. Extract info (Safely using .get() to avoid KeyErrors)
            filename = msg_content.get('filename', 'Unknown')
            chunk_idx = msg_content.get('chunk_index', 'N/A')
            machine = payload.get('machine_id', 'Unknown')
            label_val = payload.get('label', 0)
            accel_x = payload.get('accel_x', [])

            # 3. Print the Chunk-level info
            print(f"\n--- [Message {messages_processed}] Topic Index: {total_seen} ---")
            print(f"  File:      {filename}")
            print(f"  Chunk:     #{chunk_idx}")
            print(f"  Machine:   {machine}")
            print(f"  Samples:   {len(accel_x)} in this chunk")
            print(f"  Label:     {'Anomaly (1)' if label_val == 1 else 'Normal (0)'}")
            
            # Optional: Show a small slice of the data
            if accel_x:
                print(f"  Data Slice (X): {accel_x[:3]}...")

            if messages_processed >= max_messages:
                logger.info(f"Reached target of {max_messages} processed chunks.")
                break
                
    except Exception as e:
        logger.error(f"Error consuming message: {e}")
    finally:
        consumer.close()
        logger.info(f"Closed consumer. Received total of {messages_processed} messages.")

if __name__ == '__main__':
    # Verify the first 5 messages
    consume_messages(max_messages=5)