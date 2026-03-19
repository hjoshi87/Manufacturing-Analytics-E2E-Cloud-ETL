"""
Test Week 1 setup: Infrastructure, data, and Kafka
"""
import subprocess
import sys
from pathlib import Path
import time

def test_python_env():
    """Test Python environment and dependencies"""
    print("\n[1] Testing Python environment...")
    try:
        import pandas, numpy, h5py, kafka
        print(" All dependencies installed")
        return True
    except ImportError as e:
        print(f" Missing dependency: {e}")
        return False

def test_bosch_data():
    """Test Bosch dataset accessibility using verified config"""
    print("\n[2] Testing Bosch dataset...")
    from src.config import BOSCH_DATA_DIR # Import the path we just fixed
    
    if not BOSCH_DATA_DIR.exists():
        print(f" Bosch data not found at {BOSCH_DATA_DIR}")
        return False
    
    h5_files = list(BOSCH_DATA_DIR.rglob('*.h5'))
    if not h5_files:
        print(f" No HDF5 files found in {BOSCH_DATA_DIR}")
        return False
    
    print(f" Found {len(h5_files)} HDF5 files")
    return True

def test_kafka():
    """Test Kafka connectivity using config"""
    print("\n[3] Testing Kafka connectivity...")
    from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY
    import json
    from kafka import KafkaProducer
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000
        )
        # We just see if we can connect
        producer.close()
        print(f" Kafka connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return True
    except Exception as e:
        print(f" Kafka error: {e}")
        return False

def test_aws():
    """Test AWS S3 connectivity using .env credentials"""
    print("\n[4] Testing AWS connectivity...")
    from src.config import AWS_REGION, S3_BUCKET
    try:
        import boto3
        # Boto3 will automatically find your keys in the environment 
        # because load_dotenv was called in src.config
        s3 = boto3.client('s3', region_name=AWS_REGION)
        
        # Check if our specific bucket exists
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f" AWS S3 working. Connected to: {S3_BUCKET}")
        return True
            
    except Exception as e:
        print(f" AWS/S3 error: {e}")
        return False

def test_config():
    """Test configuration"""
    print("\n[5] Testing configuration...")
    try:
        sys.path.insert(0, str(Path('.')))
        from src.config import (
            BOSCH_DATA_DIR, 
            KAFKA_BOOTSTRAP_SERVERS,
            validate_config
        )
        
        validate_config()
        print(" Configuration valid")
        return True
        
    except Exception as e:
        print(f" Configuration error: {e}")
        return False

def main():
    """Run all tests"""
    print("="*60)
    print("WEEK 1 SETUP VALIDATION")
    print("="*60)
    
    results = {
        'Python Environment': test_python_env(),
        'Bosch Data': test_bosch_data(),
        'Kafka': test_kafka(),
        'AWS': test_aws(),
        'Configuration': test_config(),
    }
    
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = " PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\n{passed}/{total} tests passed")
    
    if passed == total:
        print("\n Week 1 setup complete and validated!")
        return 0
    else:
        print("\n Some tests failed. See above for details.")
        return 1

if __name__ == '__main__':
    sys.exit(main())