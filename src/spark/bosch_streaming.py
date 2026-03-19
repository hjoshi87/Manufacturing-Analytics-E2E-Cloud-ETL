"""
SPARK STREAMING IMPLEMENTATION
===============================
DISCLAIMER: This is an architectural proof-of-concept for streaming analytics.
The production pipeline uses AWS Glue batch jobs for cost efficiency.

However, this demonstrates:
- Spark Structured Streaming setup
- Real-time windowing operations
- Stream-to-Redshift integration patterns
- Handling late/out-of-order data

Benefits of Streaming vs Batch:
- Lower latency (minutes vs hours)
- Real-time anomaly alerts
- Live dashboards

To run locally:
    docker-compose up spark
    python src/spark/bosch_streaming.py

See: docs/STREAMING_ARCHITECTURE.md for full details

Spark Structured Streaming job to read Bosch data from Kafka and aggregate to 15-minute windows
"""
import sys
import os
from pathlib import Path

# Set up path for both IDE and runtime
_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_root))
os.chdir(str(_root))  # Also change working directory

# Now these imports work in IDE too
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, max, avg, stddev, 
    when, struct, explode, array, lit, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, 
    IntegerType, ArrayType, DoubleType, TimestampType
)
import logging
from datetime import datetime

from config.spark.spark_config import create_spark_session

logger = logging.getLogger(__name__)

# Define schema for Bosch Kafka messages
BOSCH_MESSAGE_SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("filename", StringType()),
    StructField("payload", StructType([
        StructField("operation", StringType()),
        StructField("machine_id", StringType()),
        StructField("run_id", StringType()),
        StructField("accel_x", ArrayType(FloatType())),
        StructField("accel_y", ArrayType(FloatType())),
        StructField("accel_z", ArrayType(FloatType())),
        StructField("label", IntegerType()),
        StructField("n_samples", IntegerType()),
        StructField("sampling_rate_hz", IntegerType())
    ]))
])

class BoschStreamingProcessor:
    def __init__(self, kafka_brokers="localhost:9092", app_name="bosch_streaming"):
        """Initialize Spark streaming processor"""
        self.spark = create_spark_session(app_name=app_name, local=True)
        self.kafka_brokers = kafka_brokers
        logger.info(f"Initialized with Kafka brokers: {kafka_brokers}")
    
    def read_kafka_stream(self, topic="machine_telemetry"):
        """
        Read streaming data from Kafka topic
        
        Args:
            topic: Kafka topic name
        
        Returns:
            DataFrame with raw Kafka messages
        """
        print(f"\n[Spark] Reading from Kafka topic: {topic}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("subscribe", topic) \
            .option("kafka.group.id", "bosch_spark_processor") \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 100) \
            .load()
        
        logger.info(f"Kafka stream reader created for topic: {topic}")
        return df
    
    def parse_kafka_messages(self, df):
        """
        Parse Kafka messages from JSON
        
        Args:
            df: Raw Kafka DataFrame
        
        Returns:
            Parsed DataFrame with proper schema
        """
        print("\n[Spark] Parsing Kafka messages...")
        
        # Parse JSON from Kafka value column
        df_parsed = df.select(
            from_json(
                col("value").cast("string"),
                BOSCH_MESSAGE_SCHEMA
            ).alias("message")
        ).select("message.*")
        
        # Convert timestamp string to timestamp
        df_parsed = df_parsed.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        logger.info("Messages parsed successfully")
        return df_parsed
    
    def explode_acceleration_data(self, df):
        """
        Explode acceleration arrays into individual rows
        
        Args:
            df: DataFrame with acceleration arrays
        
        Returns:
            DataFrame with one row per acceleration sample
        """
        print("\n[Spark] Exploding acceleration data...")
        
        # For now, keep arrays intact (we'll aggregate them)
        # Full explosion would explode each sample (too verbose)
        
        logger.info("Acceleration data structure prepared")
        return df
    
    def extract_features(self, df):
        """
        Extract statistical features from acceleration data
        
        Args:
            df: DataFrame with acceleration arrays
        
        Returns:
            DataFrame with extracted features
        """
        print("\n[Spark] Extracting features from acceleration data...")
        
        # Calculate features from arrays
        # RMS = sqrt(mean(x^2))
        # Peak = max(abs(x))
        # STD = standard deviation
        
        from pyspark.sql.functions import array_max, array_min, size
        
        # Function to calculate RMS from array
        def rms_calc(arr):
            """Calculate RMS from array of values"""
            if arr is None or len(arr) == 0:
                return 0.0
            return (sum([x**2 for x in arr]) / len(arr)) ** 0.5
        
        # Register UDF for RMS calculation
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        
        rms_udf = udf(rms_calc, DoubleType())
        
        # Extract features
        df_features = df.select(
            col("timestamp"),
            col("filename"),
            col("payload.machine_id").alias("machine_id"),
            col("payload.operation").alias("operation"),
            col("payload.run_id").alias("run_id"),
            col("payload.label").alias("label"),
            col("payload.sampling_rate_hz").alias("sampling_rate_hz"),
            
            # RMS acceleration (root mean square)
            rms_udf(col("payload.accel_x")).alias("rms_x"),
            rms_udf(col("payload.accel_y")).alias("rms_y"),
            rms_udf(col("payload.accel_z")).alias("rms_z"),
            
            # Peak acceleration (max absolute value)
            # Note: Using max of arrays directly
            lit(None).cast(DoubleType()).alias("peak_accel"),  # Placeholder
            
            # Number of samples
            col("payload.n_samples").alias("n_samples"),
            
            # Get current timestamp for processing
            lit(datetime.now().isoformat()).alias("processed_at")
        )
        
        logger.info("Features extracted")
        return df_features
    
    def aggregate_to_15min_windows(self, df):
        """
        Aggregate data to 15-minute windows
        
        Args:
            df: DataFrame with features
        
        Returns:
            Windowed DataFrame with aggregated metrics
        """
        print("\n[Spark] Aggregating to 15-minute windows...")
        
        # For streaming: use watermarking for late data
        df_watermarked = df.withWatermark("timestamp", "10 minutes")
        
        # Group by 15-minute windows and other dimensions
        df_windowed = df_watermarked.groupBy(
            window(col("timestamp"), "15 minutes"),
            col("machine_id"),
            col("operation")
        ).agg(
            # Aggregated metrics
            avg("rms_x").alias("avg_rms_x"),
            avg("rms_y").alias("avg_rms_y"),
            avg("rms_z").alias("avg_rms_z"),
            
            # Also track min/max for anomaly detection
            max("rms_x").alias("max_rms_x"),  # Simplified for now
            max("rms_y").alias("max_rms_y"),
            max("rms_z").alias("max_rms_z"),
            
            # Count records in window
            max("label").alias("has_anomaly"),  # 0=OK, 1=NOK
            
            # Metadata
            max("filename").alias("sample_file"),
            current_timestamp().alias("aggregated_at")
        )
        
        # Rename window column
        df_windowed = df_windowed.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("machine_id"),
            col("operation"),
            col("avg_rms_x"),
            col("avg_rms_y"),
            col("avg_rms_z"),
            col("max_rms_x"),
            col("max_rms_y"),
            col("max_rms_z"),
            col("has_anomaly"),
            col("aggregated_at")
        )
        
        logger.info("Data aggregated to 15-minute windows")
        return df_windowed
    
    def write_to_console(self, df, checkpoint_location="./checkpoints/console"):
        """
        Write streaming data to console (for debugging)
        
        Args:
            df: Streaming DataFrame
            checkpoint_location: Location for checkpoint
        """
        print("\n[Spark] Writing to console (debugging mode)...")
        
        query = df.writeStream \
            .format("console") \
            .option("checkpointLocation", checkpoint_location) \
            .option("truncate", False) \
            .outputMode("update") \
            .start()
        
        logger.info("Console output started")
        return query
    
    def write_to_parquet(self, df, output_path="s3a://bucket/bronze/bosch",
                         checkpoint_location="./checkpoints/parquet"):
        """
        Write streaming data to Parquet (Bronze layer)
        
        Args:
            df: Streaming DataFrame
            output_path: S3 path for Parquet output
            checkpoint_location: Location for checkpoint
        """
        print(f"\n[Spark] Writing to Parquet: {output_path}")
        
        query = df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_location) \
            .partitionBy("machine_id", "operation") \
            .outputMode("append") \
            .start()
        
        logger.info(f"Parquet writer started: {output_path}")
        return query
    
    def process_stream(self, kafka_topic="machine_telemetry",
                      output_mode="console",
                      output_path=None):
        """
        Main processing pipeline
        
        Args:
            kafka_topic: Kafka topic to read
            output_mode: "console" or "parquet"
            output_path: S3 path if output_mode is "parquet"
        """
        print("\n" + "="*60)
        print("BOSCH STRUCTURED STREAMING PIPELINE")
        print("="*60)
        
        try:
            # Read from Kafka
            df_kafka = self.read_kafka_stream(kafka_topic)
            
            # Parse JSON messages
            df_parsed = self.parse_kafka_messages(df_kafka)
            
            # Extract features
            df_features = self.extract_features(df_parsed)
            
            # Aggregate to windows
            df_aggregated = self.aggregate_to_15min_windows(df_features)
            
            # Write output
            if output_mode == "console":
                query = self.write_to_console(df_aggregated)
            elif output_mode == "parquet":
                if not output_path:
                    raise ValueError("output_path required for parquet mode")
                query = self.write_to_parquet(df_aggregated, output_path)
            else:
                raise ValueError(f"Unknown output_mode: {output_mode}")
            
            # Wait for termination
            print("\n[Spark] Streaming query started. Press Ctrl+C to stop.")
            query.awaitTermination()
            
        except KeyboardInterrupt:
            print("\n[Spark] Stopping stream...")
            query.stop()
            logger.info("Stream stopped")
        except Exception as e:
            logger.error(f"Error in pipeline: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()

if __name__ == '__main__':
    # Start processor
    processor = BoschStreamingProcessor()
    
    # Run in console mode first (for debugging)
    processor.process_stream(
        kafka_topic="machine_telemetry",
        output_mode="console"
    )

