"""
KAFKA STREAMING IMPLEMENTATION
================================

DISCLAIMER: This is an architecture exploration for real-time streaming.
Currently in production, the implementation uses AWS Glue for batch ETL.

However, this code demonstrates:
- Real-time data ingestion capability
- Kafka producer pattern
- Message serialization
- Error handling for streaming

Use Cases:
- Real-time anomaly detection (future enhancement)
- Streaming dashboards
- Live alert systems

To run:
    python src/kafka/bosch_producer.py

Note: Requires Kafka broker running (see docker-compose.yml)
"""
import h5py
import json
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from datetime import datetime, timedelta
import logging
from tqdm import tqdm
import time
root_dir = Path(__file__).resolve().parent.parent.parent
env_path = root_dir / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BoschProducer:
    def __init__(self, bootstrap_servers=os.getenv('KAFKA_BROKER')):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Increase message size limit if needed (default is 1MB)
            max_request_size=5242880  # 5MB
        )
        logger.info(f"Producer initialized for {bootstrap_servers}")
    
    def load_h5_file_raw(self, filepath):
        """Loads the raw numpy array and metadata only"""
        with h5py.File(filepath, 'r') as f:
            raw_data = f['vibration_data'][:]
            label = 1 if 'bad' in str(filepath).lower() else 0
            filename = os.path.basename(filepath)
            machine_id = filename.split('_')[0]
            return raw_data, label, machine_id

    def send_messages(self, 
                      data_dir=os.getenv('BOSCH_DATA_DIR'),
                      topic='machine_telemetry', 
                      max_files=None):
        if not data_dir:
            raise ValueError(" Data directory not found! Set BOSCH_DATA_DIR in your .env file.")
        
        
        data_path = Path(data_dir)
        h5_files = list(data_path.rglob('*.h5'))
        if max_files:
            h5_files = h5_files[:max_files]
        
        logger.info(f"Found {len(h5_files)} files. Starting chunked stream...")

        for h5_file in tqdm(h5_files, desc="Streaming to Kafka"):
            try:
                # 1. Load the data as a raw numpy array first
                raw_data, label, machine_id = self.load_h5_file_raw(str(h5_file))
                
                # 2. Chunking logic: 1000 samples = ~25KB per message (Very safe for Docker)
                chunk_size = 1000
                total_samples = len(raw_data)
                
                for i in range(0, total_samples, chunk_size):
                    chunk = raw_data[i : i + chunk_size]
                    
                    message = {
                        'timestamp': datetime.now().isoformat(),
                        'filename': h5_file.name,
                        'chunk_index': i // chunk_size,
                        'is_last_chunk': (i + chunk_size) >= total_samples,
                        'payload': {
                            'machine_id': machine_id,
                            'accel_x': chunk[:, 0].tolist(),
                            'accel_y': chunk[:, 1].tolist(),
                            'accel_z': chunk[:, 2].tolist(),
                            'label': label
                        }
                    }
                    
                    # 3. Send chunk
                    self.producer.send(topic, value=message)
                
                # Flush after each file to keep memory stable
                self.producer.flush()
                
            except Exception as e:
                logger.error(f"Error processing {h5_file.name}: {e}")
                continue
        self.producer.flush()
        
    def close(self):
        """Add this method to fix the AttributeError"""
        if self.producer:
            logger.info("Flushing & closing Kafka producer...")
            self.producer.flush() # Forces all buffered messages to be sent
            self.producer.close()

if __name__ == '__main__':
    producer = BoschProducer()
    try:
        # Pass your absolute path here
        producer.send_messages(max_files=10)
    finally:
        producer.close()

