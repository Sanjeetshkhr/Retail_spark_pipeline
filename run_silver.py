"""
Run Silver Layer Pipeline
This script serves as the entry point for running the silver layer ingestion process. It initializes the Spark session, defines the paths for the bronze and silver layers, and calls the processing function to transform the data from the bronze layer to the silver layer.
"""

from src.silver.ingest_silver import process_silver
from src.utils.spark_utils import get_spark_session, get_version
from config.config import BRONZE_PATH, SILVER_PATH
import os
import sys

sys.path.insert(0, os.path.abspath(' '))

def main():
    print("\n" + "="*20 + "\n" + " Starting Silver Layer Pipeline "+ "\n"  + "="*20 + "\n")
    
    # Show version information
    version_info = get_version()
    if version_info:
        print(f"Project Version: {version_info.get('project_version', 'Unknown')}")

    # Check if bronze path exists
    bronze_data_path = os.path.join(BRONZE_PATH, "retail_data")
    if not os.path.exists(bronze_data_path):
        print(f"Bronze Layer path does not exists at : {bronze_data_path}")
        print("Please run the bronze layer pipeline first to create the necessary data.")
        return

    print("\n" + "="*20 + "\n" + " Starting Silver Layer Processing "+ "\n"  + "="*20 + "\n")

    spark = None

    try:
        print("Step 1: Initializing Spark Session")
        # Initialize Spark session
        spark = get_spark_session()
        print(f"Spark Session initialized successfully. Version: {spark.version}\n")

        print("Step 2: Processing Silver Layer")
        # Process silver layer
        df_silver = process_silver(spark, BRONZE_PATH, SILVER_PATH)

        print("\n" + "="*20 + "\n" + " Silver Layer Pipeline completed successfully! "+ "\n"  + "="*20 + "\n")

        print(f"Output Saved at: {SILVER_PATH}/retail_data_cleaned")
        print("Total Records in Silver Layer:", df_silver.count())

    except Exception as e:
        print(f"Error occurred during Silver Layer Pipeline: {e}")
        raise e
    finally:
        if spark is not None:
            spark.stop()
            print("Spark Session stopped.")     
if __name__ == "__main__":
    main()  
