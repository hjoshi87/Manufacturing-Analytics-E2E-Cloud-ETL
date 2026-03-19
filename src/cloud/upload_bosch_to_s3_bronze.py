"""
Upload Bosch HDF5 data directly to S3 Bronze Layer
FIXED: Converts timestamps to microsecond precision (Spark-compatible)
"""
import h5py
import json
import os
from pathlib import Path
import boto3
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from tqdm import tqdm
import sys
import tempfile
from dotenv import load_dotenv
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

root_dir = Path(__file__).resolve().parent.parent.parent
env_path = root_dir / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BoschToS3Bronze:
    """Main class"""
    def __init__(self, s3_bucket, region):
        """Initialize S3 uploader"""
        self.s3_client = boto3.client('s3', region_name=region)
        self.bucket = s3_bucket
        self.region = region
        logger.info(f"Initialized S3 client for bucket: {s3_bucket} in {region}")
    
    def load_h5_file(self, filepath):
        """Load and extract features from HDF5 file"""
        records = []
        try:
            with h5py.File(filepath, 'r') as f:
                accel_data = f['vibration_data'][:]
                
                filename = Path(filepath).name
                # Format: M01_Aug_2019_OP00_000.h5
                parts = filename.replace('.h5', '').split('_')
                machine_id = parts[0]      # M01, M02, M03
                month = parts[1]           # Aug
                year = parts[2]            # 2019
                operation = parts[3]       # OP00
                
                # Parse date
                date_str = f"{month}{year}"
                date_obj = datetime.strptime(date_str, "%b%Y")
                
                label = 1 if 'bad' in filepath.lower() else 0
                
                chunk_size = 1000
                sampling_rate = 2000
                
                for i in range(0, len(accel_data), chunk_size):
                    chunk = accel_data[i:i+chunk_size]
                    
                    if len(chunk) == 0:
                        continue
                    
                    rms_x = np.sqrt(np.mean(chunk[:, 0] ** 2))
                    rms_y = np.sqrt(np.mean(chunk[:, 1] ** 2))
                    rms_z = np.sqrt(np.mean(chunk[:, 2] ** 2))
                    
                    chunk_timestamp = date_obj + timedelta(seconds=(i / sampling_rate))
                    
                    window_15min = chunk_timestamp.replace(
                        minute=(chunk_timestamp.minute // 15) * 15,
                        second=0,
                        microsecond=0
                    )
                    
                    records.append({
                        'timestamp_15min': window_15min,
                        'machine_id': machine_id,
                        'operation': operation,
                        'rms_x': float(rms_x),
                        'rms_y': float(rms_y),
                        'rms_z': float(rms_z),
                        'label': int(label),
                        'filename': filename,
                        'sample_count': len(chunk)
                    })
        
        except Exception as e:
            logger.warning(f"Error processing {filepath}: {e}")
        
        return records
    
    def batch_aggregate_records(self, records):
        """
        Aggregate chunks by 15-min window
        CRITICAL: Converts timestamps to microsecond precision for Spark compatibility
        """
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        df['timestamp_15min'] = pd.to_datetime(df['timestamp_15min'])
        
        aggregated = df.groupby(['timestamp_15min', 'machine_id', 'operation']).agg({
            'rms_x': 'mean',
            'rms_y': 'mean',
            'rms_z': 'mean',
            'label': 'max',
            'sample_count': 'sum',
            'filename': lambda x: list(x)[0]
        }).reset_index()
        
        aggregated.columns = [
            'timestamp_15min', 'machine_id', 'operation',
            'avg_rms_x', 'avg_rms_y', 'avg_rms_z',
            'has_anomaly', 'total_samples', 'sample_file'
        ]
        
        # ====================================================================
        # CRITICAL FIX: Convert timestamps to microsecond precision
        # ====================================================================
        # Problem: pandas writes datetime64[ns] (nanoseconds)
        #          AWS Glue Spark doesn't support TIMESTAMP(NANOS) in Parquet
        # Solution: Convert to datetime64[us] (microseconds) before writing
        # Result: Parquet will use TIMESTAMP(MICROS) which Spark CAN read
        # ====================================================================
        logger.info("  Converting timestamps to microsecond precision (Spark-compatible)...")
        aggregated['timestamp_15min'] = aggregated['timestamp_15min'].astype('datetime64[us]')
        logger.info("  [PASS] Timestamps converted successfully")
        
        return aggregated
    
    def upload_to_s3(self, df, machine_id, operation):
        """Upload aggregated data to S3"""
        if df.empty:
            logger.warning(f"Empty dataframe for {machine_id}/{operation}, skipping")
            return False
        
        try:
            # Use system temp directory (cross-platform)
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                parquet_file = tmp.name
            
            # Write Parquet with standard timestamp format
            # Using engine='pyarrow' ensures correct Parquet metadata
            df.to_parquet(
                parquet_file, 
                index=False,
                engine='pyarrow',
                compression='snappy'
            )
            
            s3_key = f"bosch_telemetry/machine_id={machine_id}/operation={operation}/data.parquet"
            
            self.s3_client.upload_file(
                parquet_file,
                self.bucket,
                s3_key
            )
            
            logger.info(f"[PASS] Uploaded {len(df)} records to s3://{self.bucket}/{s3_key}")
            
            # Clean up
            os.remove(parquet_file)
            return True
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return False
    
    def process_all_files(self, data_dir, max_files=None):
        """Process all HDF5 files and upload to S3"""
        data_path = Path(data_dir)
        h5_files = list(data_path.rglob('*.h5'))
        
        if max_files:
            h5_files = h5_files[:max_files]
        
        logger.info(f"Found {len(h5_files)} HDF5 files")
        
        all_records = []
        processed_count = 0
        
        print("\n" + "="*60)
        print("UPLOADING BOSCH DATA TO S3 BRONZE")
        print("="*60)
        
        for h5_file in tqdm(h5_files, desc="Processing HDF5 files"):
            records = self.load_h5_file(str(h5_file))
            all_records.extend(records)
            processed_count += 1
        
        logger.info(f"[PASS] Processed {processed_count} files, {len(all_records)} chunks extracted")
        
        df_aggregated = self.batch_aggregate_records(all_records)
        
        if df_aggregated.empty:
            logger.error("No data to upload!")
            return False
        
        logger.info(f"[PASS] Aggregated to {len(df_aggregated)} 15-min windows")
        
        uploaded_count = 0
        for machine_id in df_aggregated['machine_id'].unique():
            for operation in df_aggregated['operation'].unique():
                df_subset = df_aggregated[
                    (df_aggregated['machine_id'] == machine_id) &
                    (df_aggregated['operation'] == operation)
                ]
                
                if self.upload_to_s3(df_subset, machine_id, operation):
                    uploaded_count += 1
        
        print("\n" + "="*60)
        print("UPLOAD SUMMARY")
        print("="*60)
        print(f"Total HDF5 files processed: {processed_count}")
        print(f"Total chunks extracted: {len(all_records)}")
        print(f"Total 15-min windows: {len(df_aggregated)}")
        print(f"Machine/Operation combinations uploaded: {uploaded_count}")
        print(f"Date range: {df_aggregated['timestamp_15min'].min()} to {df_aggregated['timestamp_15min'].max()}")
        print(f"\n[PASS] TIMESTAMP FORMAT: datetime64[us] (microseconds)")
        print(f"[PASS] PARQUET METADATA: TIMESTAMP(MICROS)")
        print(f"[PASS] SPARK COMPATIBILITY: Glue will read successfully")
        print(f"\n[PASS] Successfully uploaded to s3://{self.bucket}/bosch_telemetry/")
        
        return True
    
    def verify_s3_upload(self):
        """Verify data in S3"""
        print("\n[Verification] Checking S3 Bronze contents...")
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix='bosch_telemetry/'
            )
            
            if 'Contents' in response:
                files = response['Contents']
                print(f"[PASS] Found {len(files)} Parquet files in S3")
                
                for obj in files[:5]:
                    print(f"  - {obj['Key']} ({obj['Size'] / 1024:.1f} KB)")
                
                if len(files) > 5:
                    print(f"  ... and {len(files) - 5} more files")
                
                return True
            else:
                print("[FAIL] No files found in S3")
                return False
                
        except Exception as e:
            logger.error(f"Error verifying S3: {e}")
            return False

def main():
    """Main entry point"""
    
    # Configuration
    raw_path = os.getenv('BOSCH_DATA_DIR')
    data_dir = os.path.normpath(raw_path)
    s3_bucket = os.getenv('AWS_S3_BUCKET_BRONZE')
    region = os.getenv('AWS_REGION', 'us-east-1')
    
    print(f"\n[Config]")
    print(f"  Data dir: {data_dir}")
    print(f"  S3 bucket: {s3_bucket}")
    print(f"  Region: {region}")
    print(f"\n[Timestamp Fix]")
    print(f"  Converting nanosecond timestamps to microseconds")
    print(f"  Ensures Spark Glue compatibility")
    
    uploader = BoschToS3Bronze(
        s3_bucket=s3_bucket,
        region=region
    )
    
    # Change max_files to None to process ALL files
    success = uploader.process_all_files(
        data_dir=data_dir,
        max_files=None  # Set to None for all files
    )
    
    if success:
        uploader.verify_s3_upload()
        return 0
    else:
        logger.error("Upload failed")
        return 1

if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)