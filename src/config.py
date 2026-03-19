"""
Configuration management for the project
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
root_dir = Path(__file__).resolve().parent.parent
env_path = root_dir / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
BOSCH_DATA_DIR = DATA_DIR / 'bosch' / 'raw' / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output'

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER', 'localhost:9092').split(',')
KAFKA_TOPIC_TELEMETRY = os.getenv('KAFKA_TOPIC_TELEMETRY', 'machine_telemetry')
KAFKA_TOPIC_PRICES = os.getenv('KAFKA_TOPIC_PRICES', 'energy_prices')
KAFKA_MAX_MESSAGE_BYTES = 5242880  # 5MB buffer limit

# AWS config
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET = os.getenv('AWS_S3_BUCKET_BRONZE')

# Bosch dataset config
BOSCH_SAMPLING_RATE_HZ = 2000
BOSCH_OPERATIONS = [f'OP{i:02d}' for i in range(15)]  # OP00 - OP14
CHUNK_SIZE = 1000  # Samples per Kafka message
BOSCH_MACHINES = ['M01', 'M02', 'M03']

# Processing config
BATCH_SIZE = 1000
AGGREGATION_WINDOW_MINUTES = 15

def validate_config():
    """Validate that all required paths exist"""
    if not BOSCH_DATA_DIR.exists():
        print(f"   Bosch data directory not found: {BOSCH_DATA_DIR}")
        print(f"   Run: git clone https://github.com/boschresearch/CNC_Machining.git {BOSCH_DATA_DIR.parent}")
    else:
        print(f" Bosch data found at: {BOSCH_DATA_DIR}")
    
    if not S3_BUCKET:
        print("S3_BUCKET_NAME not configured")
    else:
        print(f"S3 bucket: {S3_BUCKET}")

if __name__ == '__main__':
    validate_config()

