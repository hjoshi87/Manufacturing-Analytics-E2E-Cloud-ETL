"""
AWS Glue Job: Bronze to Silver Transformation (FINAL - PRODUCTION READY)
Reads Bosch telemetry, energy prices, and industrial data from S3 Bronze
Performs temporal alignment (15-min windows)
Writes harmonized data to S3 Silver

FIXES APPLIED:
- Removed broken window function for forward-fill
- Simple coalesce with average price for missing values
- No complex window operations
"""
import sys
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_timestamp, floor, unix_timestamp, 
    coalesce, avg, max as spark_max, min as spark_min,
    lit, trunc
)
from pyspark.sql.types import DoubleType

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

logger.info("="*70)
logger.info("GLUE JOB: BRONZE TO SILVER TRANSFORMATION (FINAL)")
logger.info("="*70)

try:
    # Configuration
    s3_bronze = "manufacturing-energy-bronze-hj-capstone-project"
    s3_silver = "manufacturing-energy-silver-hj-capstone-project"
    
    # ============================================================================
    # STEP 1: Read Bronze Layer Data
    # ============================================================================
    logger.info("\n[STEP 1] Reading Bronze layer data from S3...")
    
    # Read Bosch telemetry
    logger.info("  Reading Bosch telemetry...")
    try:
        df_bosch = spark.read.parquet(f"{s3_bronze}bosch_telemetry/")
        bosch_count = df_bosch.count()
        logger.info(f"  [PASS] Bosch records: {bosch_count:,}")
    except Exception as e:
        logger.error(f"  [FAIL] Error reading Bosch: {e}")
        raise
    
    # Read energy prices
    logger.info("  Reading energy prices...")
    try:
        df_prices = spark.read.parquet(f"{s3_bronze}energy_prices/energy_prices.parquet")
        prices_count = df_prices.count()
        logger.info(f"  [PASS] Energy price records: {prices_count:,}")
    except Exception as e:
        logger.error(f"  [FAIL] Error reading energy prices: {e}")
        raise
    
    # Read industrial data
    logger.info("  Reading industrial data...")
    try:
        df_industrial = spark.read.parquet(f"{s3_bronze}industrial_data/industrial_data.parquet")
        industrial_count = df_industrial.count()
        logger.info(f"  [PASS] Industrial records: {industrial_count:,}")
    except Exception as e:
        logger.error(f"  [FAIL] Error reading industrial data: {e}")
        raise
    
    # ============================================================================
    # STEP 2: Normalize Timestamps
    # ============================================================================
    logger.info("\n[STEP 2] Normalizing timestamps...")
    
    # Bosch: timestamp_15min is already correct from upload
    logger.info("  Processing Bosch timestamps...")
    df_bosch_normalized = df_bosch.select(
        col("timestamp_15min").cast("timestamp"),
        col("machine_id"),
        col("operation"),
        col("avg_rms_x"),
        col("avg_rms_y"),
        col("avg_rms_z"),
        col("has_anomaly"),
        col("total_samples"),
        col("sample_file")
    )
    
    # Verify timestamp
    sample_timestamp = df_bosch_normalized.select("timestamp_15min").limit(1).collect()
    if sample_timestamp:
        sample_val = sample_timestamp[0]["timestamp_15min"]
        logger.info(f"  Sample Bosch timestamp: {sample_val}")
        if "1970" not in str(sample_val):
            logger.info(f"  [PASS] Timestamp is in expected range")
        else:
            logger.error(f"  [FAIL] Timestamp shows 1970 - CORRUPT DATA!")
    
    # Energy prices: normalize to 15-min boundaries
    logger.info("  Processing energy prices timestamps...")
    df_prices_normalized = df_prices.withColumn(
        "timestamp",
        col("timestamp").cast("timestamp")
    ).withColumn(
        "timestamp_15min",
        (floor(unix_timestamp(col("timestamp")) / 900) * 900).cast("timestamp")
    ).select(
        col("timestamp_15min"),
        col("price_eur_mwh").cast(DoubleType())
    ).distinct()
    
    logger.info(f"  [PASS] Energy prices normalized to {df_prices_normalized.count():,} unique 15-min periods")
    
    # Industrial data: keep as-is with date
    logger.info("  Processing industrial data timestamps...")
    df_industrial_normalized = df_industrial.withColumn(
        "date",
        col("date").cast("timestamp")
    ).select(
        col("date"),
        col("industrial_production_index").cast(DoubleType())
    )
    
    logger.info(f"  [PASS] Industrial data: {df_industrial_normalized.count():,} records")
    
    # ============================================================================
    # STEP 3: Join Bosch with Energy Prices
    # ============================================================================
    logger.info("\n[STEP 3] Joining Bosch with energy prices...")
    
    df_bosch_prices = df_bosch_normalized.join(
        df_prices_normalized,
        on="timestamp_15min",
        how="left"
    )
    
    logger.info(f"  [PASS] Joined {df_bosch_prices.count():,} records")
    
    # Handle missing prices
    logger.info("  Checking price coverage...")
    null_prices = df_bosch_prices.filter(col("price_eur_mwh").isNull()).count()
    total_records = df_bosch_prices.count()
    null_pct = (null_prices / total_records * 100) if total_records > 0 else 0
    
    logger.info(f"  Missing prices: {null_prices} ({null_pct:.2f}%)")
    
    if null_prices > 0:
        logger.info("  Filling missing prices with average...")
        avg_price = df_bosch_prices.filter(col("price_eur_mwh").isNotNull()).agg(
            avg("price_eur_mwh")
        ).collect()[0][0]
        
        logger.info(f"  Average price: {avg_price:.2f} EUR/MWh")
        
        df_bosch_prices = df_bosch_prices.withColumn(
            "price_eur_mwh",
            coalesce(col("price_eur_mwh"), lit(avg_price))
        )
        
        logger.info(f"  [PASS] Filled missing prices with average")
    else:
        logger.info(f"  [PASS] Price coverage is 100%")
    
    # ============================================================================
    # STEP 4: Join with Industrial Data (Monthly)
    # ============================================================================
    logger.info("\n[STEP 4] Joining with industrial production index...")
    
    # Create month column for joining
    df_bosch_prices = df_bosch_prices.withColumn(
        "month",
        trunc(col("timestamp_15min"), "month")
    )
    
    df_industrial_normalized = df_industrial_normalized.withColumn(
        "month",
        trunc(col("date"), "month")
    )
    
    # Left join with industrial data
    df_final = df_bosch_prices.join(
        df_industrial_normalized,
        on="month",
        how="left"
    ).drop("month", "date")
    
    logger.info(f"  [PASS] Final joined dataset: {df_final.count():,} records")
    
    # ============================================================================
    # STEP 5: Data Quality Checks
    # ============================================================================
    logger.info("\n[STEP 5] Data quality checks...")
    
    # Check for nulls in key columns
    quality_checks = {
        'timestamp_15min': df_final.filter(col("timestamp_15min").isNull()).count(),
        'machine_id': df_final.filter(col("machine_id").isNull()).count(),
        'operation': df_final.filter(col("operation").isNull()).count(),
        'avg_rms_x': df_final.filter(col("avg_rms_x").isNull()).count(),
        'price_eur_mwh': df_final.filter(col("price_eur_mwh").isNull()).count(),
    }
    
    logger.info("  Null value counts:")
    for column, null_count in quality_checks.items():
        null_pct = (null_count / total_records * 100) if total_records > 0 else 0
        status = "[PASS]" if null_count == 0 else "[FAIL]"
        logger.info(f"    {status} {column}: {null_count} ({null_pct:.2f}%)")
    
    # Check date range
    date_range = df_final.agg(
        spark_min("timestamp_15min").alias("min_date"),
        spark_max("timestamp_15min").alias("max_date")
    ).collect()[0]
    
    logger.info(f"  Date range: {date_range['min_date']} to {date_range['max_date']}")
    
    # Verify dates are in expected range
    min_date_str = str(date_range['min_date'])
    if "2019" in min_date_str or "2020" in min_date_str or "2021" in min_date_str:
        logger.info(f"  [PASS] Date range is in expected 2019-2021 window")
    else:
        logger.error(f"  [FAIL] Date range unexpected: {min_date_str}")
    
    # ============================================================================
    # STEP 6: Data Enrichment
    # ============================================================================
    logger.info("\n[STEP 6] Data enrichment...")
    
    # Calculate estimated power and cost
    df_final = df_final.withColumn(
        "estimated_power_kwh",
        (col("avg_rms_x") + col("avg_rms_y") + col("avg_rms_z")) / 3 * 0.5
    ).withColumn(
        "energy_cost_eur",
        col("estimated_power_kwh") * col("price_eur_mwh") / 1000
    ).withColumn(
        "anomaly_risk_score",
        col("has_anomaly").cast(DoubleType())
    )
    
    logger.info("  [PASS] Added power estimation and cost calculations")
    
    # ============================================================================
    # STEP 7: Select and Prepare Silver Layer Schema
    # ============================================================================
    logger.info("\n[STEP 7] Preparing Silver layer schema...")
    
    df_silver = df_final.select(
        col("timestamp_15min"),
        col("machine_id"),
        col("operation"),
        col("avg_rms_x").alias("vibration_rms_x"),
        col("avg_rms_y").alias("vibration_rms_y"),
        col("avg_rms_z").alias("vibration_rms_z"),
        col("has_anomaly"),
        col("price_eur_mwh").alias("energy_price_eur_mwh"),
        col("industrial_production_index"),
        col("estimated_power_kwh"),
        col("energy_cost_eur"),
        col("anomaly_risk_score")
    )
    
    logger.info(f"  Silver schema columns: {', '.join(df_silver.columns)}")
    
    # ============================================================================
    # STEP 8: Write to S3 Silver Layer
    # ============================================================================
    logger.info("\n[STEP 8] Writing to S3 Silver layer...")
    
    silver_output_path = f"{s3_silver}harmonized/"
    
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("machine_id", "operation") \
        .parquet(silver_output_path)
    
    logger.info(f"  [PASS] Written to {silver_output_path}")
    
    # ============================================================================
    # STEP 9: Summary
    # ============================================================================
    logger.info("\n" + "="*70)
    logger.info("TRANSFORMATION SUMMARY")
    logger.info("="*70)
    
    logger.info("\nInput (Bronze Layer):")
    logger.info(f"  Bosch telemetry records: {bosch_count:,}")
    logger.info(f"  Energy price records: {prices_count:,}")
    logger.info(f"  Industrial data records: {industrial_count:,}")
    
    logger.info("\nOutput (Silver Layer):")
    logger.info(f"  Total records: {df_silver.count():,}")
    logger.info(f"  Unique machines: {df_silver.select('machine_id').distinct().count()}")
    logger.info(f"  Unique operations: {df_silver.select('operation').distinct().count()}")
    logger.info(f"  Date range: {date_range['min_date']} to {date_range['max_date']}")
    
    logger.info("\n[PASS] GLUE JOB COMPLETED SUCCESSFULLY")
    
except Exception as e:
    logger.error(f"\n[FAIL] ERROR: {e}", exc_info=True)
    sys.exit(1)

finally:
    job.commit()