"""
Spark ETL job for energy prices
Reads prices and prepares for joining with telemetry
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round as spark_round
import sys
sys.path.insert(0, str(__file__).replace('src/spark/energy_prices_etl.py', ''))

from config.spark.spark_config import create_spark_session
from src.energy.entso_client import ENTSOEClient

class EnergyPriceETL:
    def __init__(self):
        self.spark = create_spark_session("energy_prices_etl")
        self.entso_client = ENTSOEClient()
    
    def fetch_and_load_prices(self, country="DE", days_back=7):
        """
        Fetch prices from ENTSO-E and load into Spark DataFrame
        
        Args:
            country: Country code
            days_back: Number of days to fetch
        
        Returns:
            Spark DataFrame with prices
        """
        from datetime import datetime, timedelta
        
        print(f"\n[Energy Prices] Fetching {country} prices (last {days_back} days)...")
        
        # Get date range
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        # Fetch prices
        df_prices = self.entso_client.get_day_ahead_prices(country, start_date, end_date)
        
        # Convert to Spark DataFrame
        df_spark = self.spark.createDataFrame(df_prices)
        
        # Ensure timestamp is proper type
        df_spark = df_spark.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Round prices to 2 decimals
        df_spark = df_spark.withColumn(
            "price_eur_mwh",
            spark_round(col("price_eur_mwh"), 2)
        )
        
        print(f"[Energy Prices] Loaded {df_spark.count()} price records")
        return df_spark
    
    def aggregate_prices_to_15min(self, df_prices):
        """
        Aggregate hourly prices to 15-minute windows
        Uses forward-fill for missing 15-min intervals
        
        Args:
            df_prices: DataFrame with hourly prices
        
        Returns:
            DataFrame with 15-minute prices
        """
        from pyspark.sql.functions import window, first
        
        print("\n[Energy Prices] Aggregating to 15-minute windows...")
        
        # Create 15-minute windows
        df_windowed = df_prices.groupBy(
            window(col("timestamp"), "15 minutes")
        ).agg(
            first("price_eur_mwh").alias("price_eur_mwh"),
            first("country_code").alias("country_code"),
            first("source").alias("source")
        )
        
        # Extract window boundaries
        df_windowed = df_windowed.select(
            col("window.start").alias("timestamp"),
            col("window.end").alias("window_end"),
            col("price_eur_mwh"),
            col("country_code"),
            col("source")
        )
        
        print(f"[Energy Prices] Created {df_windowed.count()} 15-minute price records")
        return df_windowed
    
    def process(self, country="DE", output_path=None):
        """
        Main processing pipeline
        
        Args:
            country: Country code
            output_path: Optional S3 path for output
        """
        try:
            # Fetch prices
            df_prices = self.fetch_and_load_prices(country)
            
            # Show sample
            print("\n[Energy Prices] Sample prices:")
            df_prices.show(5)
            
            # Aggregate to 15-min
            df_15min = self.aggregate_prices_to_15min(df_prices)
            
            # Save if path provided
            if output_path:
                print(f"\n[Energy Prices] Writing to {output_path}")
                df_15min.write \
                    .mode("overwrite") \
                    .parquet(output_path)
            
            return df_15min
            
        finally:
            self.spark.stop()

if __name__ == '__main__':
    etl = EnergyPriceETL()
    df_prices = etl.process()
