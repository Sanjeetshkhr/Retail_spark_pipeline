"""
Silver Layer Ingestion Module: Process and clean data from the bronze layer.
Applies necessary transformations and cleansing to prepare data for Gold layer.
"""
from pyspark.sql.functions import col, trim
import os

def process_silver(spark, bronze_path, silver_path):
    """
    process_silver: Process data from the bronze layer and apply necessary transformations to create the silver layer.
    
    Args:
        spark: SparkSession object
        bronze_path: Path to the bronze layer data
        silver_path: Path to save the silver layer data

    Returns:
        df_silver: Processed DataFrame for the silver layer
    """

    print("\n" + "="*20 + "\n" + " Starting Silver Layer Ingestion "+ "\n"  + "="*20 + "\n")


    # Read bronze layer data
    input_path = os.path.join(bronze_path, "retail_data")
    print(f"Reading bronze data from: {input_path}")
    df_bronze = spark.read.parquet(input_path)

    print("\n Original Schema:")
    df_bronze.printSchema()

    # Apply transformations and cleansing
    df_silver = df_bronze \
        .filter(col("quantity") > 0) \
        .filter(col("InvoiceNo").isNotNull()) \
        .filter(col("UnitPrice") > 0) \
        .withColumn("Description", trim(col("Description"))) \
        .withColumn("Country", trim(col("Country")))
    
    # Add calculated columns
    df_silver = df_silver.withColumn("TotalAmount", col("quantity") * col("UnitPrice"))
    
    # Show sample of processed data
    print("\n Sample of Silver Layer Data:")
    df_silver.show(5, truncate=False)   

    # Counting the number of records in the silver layer
    records_before = df_bronze.count()   
    records_after = df_silver.count()
    print("Data Quality Report:")
    print(f"Total records in Bronze layer: {records_before}")
    print(f"Total records in Silver layer: {records_after}")
    print(f"Records removed during Silver processing: {records_before - records_after}")
    print(f"Percentage of records removed: {(records_before - records_after) / records_before * 100:.2f}%")

    # Save the processed silver layer data
    output_path = os.path.join(silver_path, "retail_data_cleaned")
    df_silver.write\
        .mode("overwrite")\
        .parquet(output_path)
    print(f"\nSaving silver data to: {output_path}")

    return df_silver    