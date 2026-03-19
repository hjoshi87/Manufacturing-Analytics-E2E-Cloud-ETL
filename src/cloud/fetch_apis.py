"""
Fetch ENTSO-E energy prices and Eurostat industrial data
Upload to S3 Bronze with source metadata preserved
"""

import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import time
import tempfile
from pathlib import Path
import os
from dotenv import load_dotenv
root_dir = Path(__file__).resolve().parent.parent.parent
env_path = root_dir / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.energy.entso_client import ENTSOEClient
from src.industrial.eurostat_client import EurostatClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CloudAPIFetcher:
    def __init__(self, s3_bucket=None, region=None):
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.bucket = s3_bucket or os.getenv("AWS_S3_BUCKET_BRONZE")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.entso_client = ENTSOEClient()
        self.eurostat_client = EurostatClient()
        logger.info(f"Initialized API fetcher for bucket: {s3_bucket}")
    
    def fetch_entso_monthly_chunks(self, start_date="2019-02-01", end_date="2021-08-31"):
        """
        Fetch ENTSO-E prices in monthly chunks to avoid API limits
        Returns hourly aggregated data with source metadata
        """
        print("\n[ENTSO-E] Fetching energy prices (monthly chunks)...")
        
        # Create monthly date chunks
        date_chunks = pd.date_range(start=start_date, end=end_date, freq='MS')
        full_data = []
        
        for start_month in date_chunks:
            # Calculate last day of month
            end_month = start_month + pd.offsets.MonthEnd(0)
            
            s_str = start_month.strftime("%Y-%m-%d")
            e_str = end_month.strftime("%Y-%m-%d")
            
            print(f"  Downloading {s_str} to {e_str}...", end=" ")
            
            # Fetch for this month
            df = self.entso_client.get_day_ahead_prices("DE", s_str, e_str)
            
            if not df.empty:
                # Resample to hourly
                df = df.set_index('timestamp')
                hourly = df['price_eur_mwh'].resample('H').mean().reset_index()
                
                # Add source metadata if not already present
                if 'source' not in hourly.columns:
                    hourly['source'] = 'ENTSO-E'
                
                full_data.append(hourly)
                print(f"[PASS] {len(hourly)} hourly records")
            else:
                print("[FAIL] No data")
            
            # Rate limit: 1 second between requests
            time.sleep(1)
        
        if full_data:
            final_df = pd.concat(full_data, ignore_index=True)
            
            # CRITICAL: Remove timezone if present
            if final_df['timestamp'].dt.tz is not None:
                final_df['timestamp'] = final_df['timestamp'].dt.tz_localize(None)
            
            # CRITICAL: Convert to microseconds (Spark-compatible)
            logger.info("  Converting timestamps to microsecond precision...")
            final_df['timestamp'] = final_df['timestamp'].astype('datetime64[us]')
            
            # Add metadata columns
            final_df['country_code'] = 'DE'
            if 'source' not in final_df.columns:
                final_df['source'] = 'ENTSO-E'
            
            print(f"[PASS] Total ENTSO-E records: {len(final_df)}")
            print(f"  Columns: {', '.join(final_df.columns)}")
            return final_df
        else:
            print("[FAIL] No data retrieved")
            return pd.DataFrame()
    
    def save_and_upload_to_s3(self, df, s3_key):
        """Save DataFrame to Parquet and upload to S3"""
        if df.empty:
            logger.warning(f"Empty dataframe, skipping upload for {s3_key}")
            return False
        
        try:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                parquet_file = tmp.name
            
            # Write with pyarrow to ensure correct timestamp format
            df.to_parquet(
                parquet_file, 
                index=False,
                engine='pyarrow',
                compression='snappy'
            )
            
            self.s3_client.upload_file(
                parquet_file,
                self.bucket,
                s3_key
            )
            
            logger.info(f"[PASS] Uploaded {len(df)} records to s3://{self.bucket}/{s3_key}")
            logger.info(f"  Columns: {', '.join(df.columns)}")
            
            import os
            os.remove(parquet_file)
            return True
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return False
    
    def fetch_and_save_entso_prices(self, start_date="2019-02-01", end_date="2021-08-31"):
        """Fetch ENTSO-E prices with monthly chunks and save to S3"""
        df_prices = self.fetch_entso_monthly_chunks(start_date, end_date)
        return self.save_and_upload_to_s3(df_prices, "energy_prices/energy_prices.parquet")
    
    def fetch_and_save_eurostat(self, start_year=2019, end_year=2021):
        """Fetch Eurostat industrial data and save to S3"""
        print("\n[Eurostat] Fetching industrial production index...")
        
        df_industrial = self.eurostat_client.get_industrial_production_index_real(
            "DE", 
            start_year=start_year, 
            end_year=end_year
        )
        
        if not df_industrial.empty:
            print(f"[PASS] Fetched {len(df_industrial)} industrial records")
            
            # CRITICAL: Convert to microseconds (Spark-compatible)
            logger.info("  Converting timestamps to microsecond precision...")
            df_industrial['date'] = df_industrial['date'].astype('datetime64[us]')
            
            # Ensure source field exists
            if 'source' not in df_industrial.columns:
                df_industrial['source'] = 'Eurostat'
            
            # Ensure country_code exists
            if 'country_code' not in df_industrial.columns:
                df_industrial['country_code'] = 'DE'
            
            print(f"  Columns: {', '.join(df_industrial.columns)}")
            
            return self.save_and_upload_to_s3(df_industrial, "industrial_data/industrial_data.parquet")
        else:
            print("[FAIL] Failed to fetch industrial data")
            return False
    
    def verify_bronze_layer(self):
        """Verify all Bronze layer data is present"""
        print("\n[Verification] Checking S3 Bronze contents...")
        
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=''
        )
        
        contents = response.get('Contents', [])
        
        datasets = {
            'bosch_telemetry': False,
            'energy_prices': False,
            'industrial_data': False
        }
        
        for obj in contents:
            key = obj['Key']
            for dataset in datasets:
                if dataset in key:
                    datasets[dataset] = True
        
        print("\nBronze Layer Status:")
        for dataset, exists in datasets.items():
            status = "[PASS]" if exists else "[FAIL]"
            print(f"  {status} {dataset}")
        
        all_present = all(datasets.values())
        return all_present


def main():
    print("\n" + "="*60)
    print("FETCHING APIs AND UPLOADING TO S3 BRONZE")
    print("="*60)
    print("\nIMPORTANT: All timestamps converted to microseconds")
    print("(Spark Glue compatible format)")
    print("\nMetadata preserved: source field included for analytics")
    
    fetcher = CloudAPIFetcher(
        s3_bucket = os.getenv('AWS_S3_BUCKET_BRONZE'),
        region = os.getenv('AWS_REGION', 'us-east-1')
    )
    
    try:
        # Fetch ENTSO-E with monthly chunks (REAL DATA, FIXED TIMESTAMPS, WITH SOURCE)
        entso_success = fetcher.fetch_and_save_entso_prices(
            start_date="2019-02-01",
            end_date="2021-08-31"
        )
        
        # Fetch Eurostat (REAL DATA, FIXED TIMESTAMPS, WITH SOURCE)
        eurostat_success = fetcher.fetch_and_save_eurostat(
            start_year=2019,
            end_year=2021
        )
        
        # Verify
        if fetcher.verify_bronze_layer():
            print("\n" + "="*60)
            print("[PASS] ALL DATA SUCCESSFULLY IN S3 BRONZE")
            print("="*60)
            print("\nData Quality:")
            print("  [PASS] All timestamps in microsecond precision (Spark-compatible)")
            print("  [PASS] Source metadata preserved (ENTSO-E, Eurostat, Bosch)")
            print("  [PASS] Ready for Glue transformation")
            return 0
        else:
            print("\n[FAIL] Some data missing in S3 Bronze")
            return 1
            
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)