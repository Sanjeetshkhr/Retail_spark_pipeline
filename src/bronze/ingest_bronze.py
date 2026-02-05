"""
Bronze Layer: Ingest raw data from sources.
Loads raw data into the bronze layer without any/minimal transformations.
"""

from pyspark.sql.functions import current_timestamp, input_file_name
import os

def process_bronze(spark, source_path, bronze_path):
    """
    Process Bronze Layer: Ingest raw data into bronze layer

    Args:
        spark: SparkSession object
        source_path: Path to the raw data source
        bronze_path: Path to save the bronze layer data

    Returns:
        DataFrame: Ingested bronze layer DataFrame
    """

    print("\n" + "="*50 + "\n")
    print("Starting Bronze Layer Ingestion Process")
    print("\n" + "="*50 + "\n")

    # Read raw data
    print(f"Reading raw data from {source_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

    # Add metadata columns
    df_bronze = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name())
    
    # Show sample data
    print("\nSample data after ingestion:")
    df_bronze.show(5, truncate=False)
    print(f"Total records ingested: {df_bronze.count()}")

    # Save to bronze layer
    bronze_path = os.path.join(bronze_path, "retail_data")
    df_bronze.write \
        .mode("overwrite") \
        .parquet(bronze_path)
    
    print(f"\nBronze layer data saved to {bronze_path}")

    return df_bronze




    
    