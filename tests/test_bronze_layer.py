"""
Test Bronze layer output
Validate data quality and completeness
"""
from pyspark.sql import SparkSession
from pathlib import Path
import sys

sys.path.insert(0, str(__file__).replace('tests/test_bronze_layer.py', ''))
from config.spark.spark_config import create_spark_session

def validate_bronze_layer():
    """Validate Bronze layer data"""
    
    spark = create_spark_session("bronze_validation", local=True)
    
    print("\n" + "="*60)
    print("BRONZE LAYER VALIDATION")
    print("="*60)
    
    results = []
    
    # Check each Bronze dataset
    datasets = [
        ("bosch_telemetry", "./data/bronze/bosch_telemetry"),
        ("energy_prices", "./data/bronze/energy_prices"),
        ("harmonized", "./data/bronze/harmonized")
    ]
    
    for name, path in datasets:
        try:
            # Check if path exists
            if not Path(path).exists():
                print(f"\n {name}: Path not found")
                results.append((name, "FAIL", "Path not found"))
                continue
            
            # Read Parquet
            df = spark.read.parquet(path)
            
            record_count = df.count()
            schema = df.schema
            columns = df.columns
            
            print(f"\n {name}")
            print(f"  Records: {record_count}")
            print(f"  Columns: {len(columns)}")
            print(f"  Null values: {sum([df.filter(df[col].isNull()).count() for col in columns])}")
            
            if record_count > 0:
                df.show(2)
                results.append((name, "PASS", f"{record_count} records"))
            else:
                results.append((name, "WARN", "0 records"))
                
        except Exception as e:
            print(f"\n {name}: {e}")
            results.append((name, "FAIL", str(e)))
    
    spark.stop()
    
    # Summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    for dataset, status, detail in results:
        symbol = "PASSED" if status == "PASS" else "FAIL" if status == "FAIL" else "FAILED"
        print(f"{symbol} {dataset:20} {status:8} {detail}")
    
    passed = sum(1 for _, status, _ in results if status == "PASS")
    total = len(results)
    
    print(f"\n{passed}/{total} datasets validated")
    
    return all(status != "FAIL" for _, status, _ in results)

if __name__ == '__main__':
    success = validate_bronze_layer()
    sys.exit(0 if success else 1)