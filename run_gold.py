"""
Run Gold layer Pipeline
"""
import sys
import os

# Add current directory to the path
sys.path.insert(0, os.path.abspath('.'))

from src.utils.spark_utils import get_spark_session, get_version
from src.gold.ingest_gold import process_gold
from config.config import SILVER_PATH, GOLD_PATH

def main():
    print("\n" + "="*20 + "\n" + " Gold Layer Pipeline "+ "\n"  + "="*20 + "\n")

    # Show version
    version_info = get_version()
    if version_info:
        print("Version Information")
        for key, value in version_info.items():
            print(f"{key} : {value}")
    
    # Check if silver path exists
    silver_data = os.path.join(SILVER_PATH, "retail_data_cleaned")
    if not os.path.exists(silver_data):
        print(f"Error: Silver layer data not found")
        print(f"Silver Layer data Path: {silver_data}")
        print(f"Please run the silver layer first to generate data for gold layer")
        return
    
    print("\n" + "="*20 + "\n" + " Starting Gold Layer Processing "+ "\n"  + "="*20 + "\n")
    spark = None

    try:
        # Step 1: Initialize Spark
        print("Step 1: Initializing the Spark session")
        spark = get_spark_session()
        print(f"Spark Session created: {spark.version}")

        # Step 2: Process Gold Layer
        print("Step 2: Processing the gold layer data")
        Sales_by_country, Top_products = process_gold(spark=spark, silver_path=SILVER_PATH, gold_path=GOLD_PATH)

        print("\n" + "="*20 + "\n" + " Gold Layer Processing completed Successfully "+ "\n"  + "="*20 + "\n")
        print(f"Output Saved at \n {GOLD_PATH}/sales_by_country \n {GOLD_PATH}\top_products")

        print("Tables created for Analytics")
        print(f" Sales By Country: {Sales_by_country.count()} countries")
        print(f" Top 10 Products selling out of {Top_products.count()} products")
        Top_products.show(10, truncate=False)

    except Exception as e:
        print(f"Raised Error: {str(e)}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            print("Spark Session Stopped")

if __name__ == "__main__":
    main()