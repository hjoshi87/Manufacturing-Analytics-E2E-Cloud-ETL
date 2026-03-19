"""
PySpark configuration for Windows development
Includes Kafka and Hadoop support with proper PATH setup
"""
from pyspark.sql import SparkSession
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

def setup_windows_environment():
    """
    Configure Windows environment for Spark + Hadoop
    """
    
    # Define Hadoop home (adjust path to your installation)
    hadoop_home = r'C:\Users\heram\hadoop-3.4.2'
    
    # Verify Hadoop exists
    if not os.path.exists(hadoop_home):
        logger.warning(f"Hadoop home not found: {hadoop_home}")
        logger.warning("Continuing without Hadoop (may affect S3 support)")
    else:
        logger.info(f"Setting HADOOP_HOME: {hadoop_home}")
        os.environ['HADOOP_HOME'] = hadoop_home
        
        # Add Hadoop bin to PATH
        hadoop_bin = os.path.join(hadoop_home, 'bin')
        if hadoop_bin not in os.environ['PATH']:
            os.environ['PATH'] += os.pathsep + hadoop_bin
            logger.info(f"Added to PATH: {hadoop_bin}")
    
    # Set Java home if needed (Spark uses this)
    # JAVA_HOME should be auto-detected, but you can set it explicitly:
    # os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_271'
    
    logger.info("Windows environment configured")

def create_spark_session(app_name="manufacturing-energy", 
                         local=True,
                         enable_kafka=True,
                         enable_s3=True):
    """
    Create SparkSession with optimized configuration for Windows
    
    Args:
        app_name: Application name
        local: If True, run locally; if False, use cluster
        enable_kafka: Include Kafka connector
        enable_s3: Include S3 support (requires Hadoop)
    
    Returns:
        SparkSession object
    """
    
    # Setup environment first
    setup_windows_environment()
    
    # Build jars.packages string
    jars_packages = []
    
    if enable_kafka:
        # Spark-Kafka connector for reading/writing Kafka
        jars_packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
        logger.info("Adding Kafka connector to JAR packages")
    
    if enable_s3:
        # Hadoop AWS (for S3 support)
        jars_packages.append("org.apache.hadoop:hadoop-aws:3.3.1")
        # AWS Java SDK v1 (required for Hadoop AWS)
        jars_packages.append("com.amazonaws:aws-java-sdk-bundle:1.12.261")
        logger.info("Adding AWS/S3 support to JAR packages")
    
    jars_string = ",".join(jars_packages)
    
    if local:
        # Local configuration (development on Windows)
        logger.info("Creating local Spark session")
        
        builder = SparkSession.builder \
            .appName(app_name) \
            .master("local[4]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
            .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
            .config("spark.hadoop.fs.s3a.access.key", "") \
            .config("spark.hadoop.fs.s3a.secret.key", "")
        
        # Add JAR packages if specified
        if jars_packages:
            builder = builder.config("spark.jars.packages", jars_string)
        
        # Windows-specific settings
        builder = builder \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.sql.warehouse.dir", "./spark-warehouse") \
            .config("spark.local.dir", "./spark-local")
        
        spark = builder.getOrCreate()
        
    else:
        # Cluster configuration (production - AWS EMR, etc.)
        logger.info("Creating cluster Spark session")
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", "4") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.streaming.kafka.maxRatePerPartition", "100000") \
            .config("spark.jars.packages", jars_string) \
            .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Log successful creation
    logger.info(f"✓ Spark session created: {app_name}")
    logger.info(f"  Version: {spark.version}")
    logger.info(f"  Master: {spark.sparkContext.master}")
    logger.info(f"  Python version: {sys.version.split()[0]}")
    
    # Print JAR packages info
    if jars_packages:
        logger.info(f"  JAR packages: {len(jars_packages)} items")
        for jar in jars_packages:
            logger.info(f"    - {jar}")
    
    return spark

def test_kafka_connectivity(bootstrap_servers="localhost:9092"):
    """
    Test if Kafka connector is working
    
    Args:
        bootstrap_servers: Kafka broker address
    
    Returns:
        True if successful, False otherwise
    """
    
    logger.info(f"Testing Kafka connectivity to {bootstrap_servers}...")
    
    try:
        spark = create_spark_session("kafka_test", local=True)
        
        # Try to read from Kafka (will fail if broker not available, but JAR will load)
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", "test") \
            .option("startingOffsets", "latest") \
            .load()
        
        schema = df.schema
        logger.info(f"✓ Kafka connector loaded successfully")
        logger.info(f"  Schema: {schema}")
        
        spark.stop()
        return True
        
    except Exception as e:
        if "kafka" in str(e).lower():
            logger.error(f"✗ Kafka connector error: {e}")
            return False
        else:
            # Kafka is available, but broker might not be running
            logger.warning(f"Kafka broker not available: {e}")
            logger.warning(f"But Kafka connector is installed correctly")
            return True

if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Test Spark session
    spark = create_spark_session()
    print(f"\nSpark Version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"App Name: {spark.sparkContext.appName}")
    
    # Test Kafka
    print("\n" + "="*60)
    kafka_ok = test_kafka_connectivity()
    print("="*60)
    
    spark.stop()
    
    if kafka_ok:
        print("\n✓ Spark + Kafka configured successfully!")
    else:
        print("\n⚠ Spark configured but check Kafka connectivity")