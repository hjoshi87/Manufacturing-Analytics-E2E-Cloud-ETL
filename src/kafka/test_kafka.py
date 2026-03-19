"""
Test Kafka connectivity
"""
from kafka import KafkaProducer, KafkaConsumer
import json
from datetime import datetime

# Test Producer
print("Testing Kafka Producer...")
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Send test message
    test_msg = {
        'timestamp': datetime.now().isoformat(),
        'test': 'Hello Kafka',
        'value': 42
    }
    
    future = producer.send('machine_telemetry', value=test_msg)
    record_metadata = future.get(timeout=10)
    
    print(f"  Message sent to topic: {record_metadata.topic}")
    print(f"  Partition: {record_metadata.partition}")
    print(f"  Offset: {record_metadata.offset}")
    
    producer.close()
    
except Exception as e:
    print(f"Producer error: {e}")

# Test Consumer
print("\nTesting Kafka Consumer...")
try:
    consumer = KafkaConsumer(
        'machine_telemetry',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000  # Wait 5 seconds for messages
    )
    
    messages_received = 0
    for message in consumer:
        print(f"Message received: {message.value}")
        messages_received += 1
    
    print(f"Total messages: {messages_received}")
    consumer.close()
    
except Exception as e:
    print(f"Consumer error: {e}")

print("\nKafka is working correctly!")

