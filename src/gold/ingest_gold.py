"""
Gold Ingest file for ingesting and creating business level data in the gold layer.
This script serves as the entry point for running the gold layer ingestion process. 
It initializes the Spark session, defines the paths for the silver and gold layers, 
and calls the processing function to transform the data from the silver layer to the gold layer.
"""
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round
import os

def process_gold(spark, silver_path, gold_path):
    """
    Process data for gold layer by ingesting data from silver layer

    Args:
        spark: SparkSession object
        silver_path: Path to the silver layer data
        gold_path: Path to save the processed gold layer data
    """
    print("\n" + "="*20 + "\n" + " Starting Gold Layer Processing "+ "\n"  + "="*20 + "\n")
    
    # Read silver layer data
    input_path = os.path.join(silver_path, "retail_data_cleaned")
    print("Reading Silver Layer data from:", input_path)
    df = spark.read.parquet(input_path)

    # Aggegration 1: Total Sales by Country
    print("Processing Total Sales by Country")
    sales_by_country = df.groupBy("country").agg(
        _sum("TotalAmount").alias("TotalSales"),
        count("InvoiceNo").alias("TotalOrders"),
        _round(avg("TotalAmount"), 2).alias("AverageOrderValue")
    ) \
    .orderBy(col("TotalSales").desc())
    print("Total Sales by Country processed successfully.")
    sales_by_country.show(10, truncate=False)

    # Aggregation 2: Top 10 Products by Sales
    print("Processing Top 10 Products by Sales")
    top_products = df.groupBy("StockCode", "Description").agg(
        _sum("Quantity").alias("TotalQuantity"),
        _sum("TotalAmount").alias("TotalRevenue")
    ) \
    .orderBy(col("TotalQuantity").desc())
    
    print("Top Products by Sales processed successfully.")
    top_products.show(10, truncate=False)

    # save the result to gold layer
    output_path_country = os.path.join(gold_path, "sales_by_country")
    sales_by_country.write.mode("overwrite").parquet(output_path_country)
    print("Sales by Country written to:", output_path_country)

    output_path_product = os.path.join(gold_path, "top_products")
    top_products.write.mode("overwrite").parquet(output_path_product)

    print ("Top Products written to:", output_path_product)

    return sales_by_country, top_products