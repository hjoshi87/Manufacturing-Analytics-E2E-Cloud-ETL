"""
Complete ETL pipeline combining all components
Bosch telemetry + Energy prices → Bronze layer (S3 Parquet)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce
import sys
import logging
from datetime import datetime
import os
sys.path.insert(0, str(__file__).replace('src/spark/complete_etl_pipeline.py', ''))

from config.spark.spark_config import create_spark_session
from src.spark.bosch_streaming import BoschStreamingProcessor
from src.spark.energy_prices_etl import EnergyPriceETL
from src.spark.temporal_alignment import TemporalHarmonizer
from pathlib import Path
from dotenv import load_dotenv
root_dir = Path(__file__).resolve().parent.parent.parent
env_path = root_dir / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ManufacturingEnergyETL:
    """
    Complete ETL pipeline for manufacturing energy analytics
    """
    
    def __init__(self, kafka_brokers="localhost:9092",
                 s3_bucket="manufacturing-energy",
                 use_local=True):
        """
        Initialize ETL pipeline
        
        Args:
            kafka_brokers: Kafka broker addresses
            s3_bucket: S3 bucket name
            use_local: Use local storage (not S3) for testing
        """
        self.spark = create_spark_session("manufacturing_energy_etl", local=use_local)
        self.kafka_brokers = kafka_brokers
        self.s3_bucket = s3_bucket
        self.use_local = use_local
        
        logger.info(f"ETL pipeline initialized")
        logger.info(f"  Kafka: {kafka_brokers}")
        logger.info(f"  S3 Bucket: {s3_bucket}")
        logger.info(f"  Local mode: {use_local}")
    
    def get_bronze_path(self, data_type="bosch"):
        """Get Bronze layer path"""
        if self.use_local:
            return f"./data/bronze/{data_type}"
        else:
            return f"s3a://{self.s3_bucket}/bronze/{data_type}"
    
    def get_silver_path(self, data_type="harmonized"):
        """Get Silver layer path"""
        if self.use_local:
            return f"./data/silver/{data_type}"
        else:
            return f"s3a://{self.s3_bucket}/silver/{data_type}"
    
    def process_bosch_batch(self, input_path=os.getenv('BOSCH_DATA_DIR'),
                           max_files=10):
        """
        Process Bosch HDF5 files in batch mode (for testing)
        
        Args:
            input_path: Path to Bosch data directory
            max_files: Max files to process
        
        Returns:
            Spark DataFrame
        """
        print("\n[ETL] Processing Bosch data files (batch mode)...")
        
        from pathlib import Path
        import h5py
        import pandas as pd
        
        data_path = Path(input_path)
        h5_files = list(data_path.rglob('*.h5'))[:max_files]
        
        records = []
        
        for h5_file in h5_files:
            try:
                with h5py.File(h5_file, 'r') as f:
                    op_key = list(f.keys())[0]
                    machine_key = list(f[op_key].keys())[0]
                    group = f[op_key][machine_key]
                    
                    # Extract data
                    accel_x = group['accel_x'][:]
                    accel_y = group['accel_y'][:]
                    accel_z = group['accel_z'][:]
                    label = group['label'][()] if 'label' in group else None
                    
                    # Calculate RMS
                    rms_x = (sum([x**2 for x in accel_x]) / len(accel_x)) ** 0.5
                    rms_y = (sum([y**2 for y in accel_y]) / len(accel_y)) ** 0.5
                    rms_z = (sum([z**2 for z in accel_z]) / len(accel_z)) ** 0.5
                    
                    # Parse metadata
                    filename = h5_file.name
                    parts = filename.replace('.h5', '').split('_')
                    machine_id = parts[0]
                    operation = op_key
                    
                    records.append({
                        'timestamp': datetime.now().isoformat(),
                        'machine_id': machine_id,
                        'operation': operation,
                        'rms_x': float(rms_x),
                        'rms_y': float(rms_y),
                        'rms_z': float(rms_z),
                        'label': int(label) if label is not None else None,
                        'n_samples': len(accel_x),
                        'filename': filename
                    })
                    
            except Exception as e:
                logger.warning(f"Error processing {h5_file.name}: {e}")
                continue
        
        # Convert to Spark DataFrame
        df_pandas = pd.DataFrame(records)
        df_spark = self.spark.createDataFrame(df_pandas)
        
        logger.info(f"Processed {len(records)} Bosch records")
        return df_spark
    
    def process_energy_prices(self, country="DE", days_back=7):
        """
        Process energy prices
        
        Args:
            country: Country code
            days_back: Days of historical data
        
        Returns:
            Spark DataFrame
        """
        print(f"\n[ETL] Processing energy prices...")
        
        etl_energy = EnergyPriceETL()
        df_prices = etl_energy.fetch_and_load_prices(country, days_back)
        
        return df_prices
    
    def aggregate_bosch_data(self, df_bosch):
        """
        Aggregate Bosch data to 15-minute windows
        
        Args:
            df_bosch: Raw Bosch DataFrame
        
        Returns:
            Aggregated DataFrame
        """
        from pyspark.sql.functions import (
            window, avg, col, to_timestamp
        )
        
        print("\n[ETL] Aggregating Bosch data to 15-minute windows...")
        
        # Convert timestamp
        df_bosch = df_bosch.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Aggregate
        df_agg = df_bosch.groupBy(
            window(col("timestamp"), "15 minutes"),
            col("machine_id"),
            col("operation")
        ).agg(
            avg(col("rms_x")).alias("avg_rms_x"),
            avg(col("rms_y")).alias("avg_rms_y"),
            avg(col("rms_z")).alias("avg_rms_z"),
            col("label").alias("has_anomaly")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("machine_id"),
            col("operation"),
            col("avg_rms_x"),
            col("avg_rms_y"),
            col("avg_rms_z"),
            col("has_anomaly")
        )
        
        logger.info(f"Aggregated to {df_agg.count()} 15-minute records")
        return df_agg
    
    def harmonize_data(self, df_telemetry, df_prices):
        """
        Harmonize telemetry and prices
        
        Args:
            df_telemetry: Aggregated telemetry
            df_prices: Energy prices
        
        Returns:
            Harmonized DataFrame
        """
        print("\n[ETL] Harmonizing data sources...")
        
        # Use temporal alignment
        df_harmonized = TemporalHarmonizer.create_harmonized_dataset(
            df_telemetry,
            df_prices
        )
        
        return df_harmonized
    
    def write_bronze_layer(self, df_bosch, df_prices, df_harmonized):
        """
        Write data to Bronze layer (raw data)
        
        Args:
            df_bosch: Raw Bosch data
            df_prices: Raw prices
            df_harmonized: Harmonized data
        """
        print("\n[ETL] Writing Bronze layer...")
        
        # Write raw Bosch data
        bosch_path = self.get_bronze_path("bosch_telemetry")
        df_bosch.write \
            .mode("append") \
            .partitionBy("machine_id") \
            .parquet(bosch_path)
        logger.info(f"Bronze (Bosch) written: {bosch_path}")
        
        # Write raw prices
        prices_path = self.get_bronze_path("energy_prices")
        df_prices.write \
            .mode("append") \
            .parquet(prices_path)
        logger.info(f"Bronze (Prices) written: {prices_path}")
        
        # Write harmonized data
        harmonized_path = self.get_bronze_path("harmonized")
        df_harmonized.write \
            .mode("append") \
            .partitionBy("machine_id", "operation") \
            .parquet(harmonized_path)
        logger.info(f"Bronze (Harmonized) written: {harmonized_path}")
    
    def run_batch_pipeline(self):
        """
        Run complete batch ETL pipeline
        """
        print("\n" + "="*80)
        print("MANUFACTURING ENERGY - BATCH ETL PIPELINE")
        print("="*80)
        
        try:
            # 1. Process Bosch data
            df_bosch = self.process_bosch_batch(max_files=10)
            print(f"\n[Summary] Bosch records: {df_bosch.count()}")
            df_bosch.show(3)
            
            # 2. Process energy prices
            df_prices = self.process_energy_prices()
            print(f"\n[Summary] Price records: {df_prices.count()}")
            df_prices.show(3)
            
            # 3. Aggregate Bosch data
            df_bosch_agg = self.aggregate_bosch_data(df_bosch)
            print(f"\n[Summary] Aggregated Bosch records: {df_bosch_agg.count()}")
            df_bosch_agg.show(3)
            
            # 4. Harmonize
            df_harmonized = self.harmonize_data(df_bosch_agg, df_prices)
            print(f"\n[Summary] Harmonized records: {df_harmonized.count()}")
            df_harmonized.show(5)
            
            # 5. Write Bronze layer
            self.write_bronze_layer(df_bosch, df_prices, df_harmonized)
            
            print("\n" + "="*80)
            print(" BATCH PIPELINE COMPLETE")
            print("="*80)
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()

if __name__ == '__main__':
    pipeline = ManufacturingEnergyETL(use_local=True)
    pipeline.run_batch_pipeline()