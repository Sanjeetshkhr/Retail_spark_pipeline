"""
Run bronze Layer Pipeline.
"""

import os
import sys

# Add current directory to path
sys.path.insert(0, os.path.abspath('.'))

from src.utils.spark_utils import get_spark_session, download_dataset, get_version
from src.bronze.ingest_bronze import process_bronze
from config.config import DATASET_URL, DATASET_NAME, BRONZE_PATH

def main():
    print("\n" + "="*50 + "\n")
    print(f"Bronze Layer Pipeline")
    print("\n" + "="*50 + "\n")

    # Show version information
    version_info = get_version()
    if version_info:
        print(f"Version Information:\n")
        for key, value in version_info.items():
            print(f"  {key}: {value}")
    print("\n" + "="*50 + "\n")
    print(f"Starting Bronze Layer Pipeline...   \n")
    print("\n" + "="*50 + "\n")

    spark = None
    try:
        # Step 1: Create Spark Session
        print("Step[1/3] Creating Spark Session...")
        spark = get_spark_session()
        print("Spark Session created successfully. {spark.version}\n")
        
        # Step 2: Download Dataset
        print("Step[2/3] Downloading Dataset...")
        source_file = os.path.join(BRONZE_PATH, f"{DATASET_NAME}.csv")
        download_dataset(DATASET_URL, source_file)

        # Step 3: Process Bronze Layer
        print("Step[3/3] Processing Bronze Layer...")
        df_bronze = process_bronze(spark, source_file, BRONZE_PATH)
        print("\nBronze Layer Pipeline completed successfully!\n")
        print("\n" + "="*50 + "\n")
        print("Output Bronze DataFrame Schema:")
        df_bronze.printSchema()
        print("\n" + "="*50 + "\n")
        print('Totoal Records in Bronze DataFrame:', df_bronze.count())


    except Exception as e:
        print(f"Error occurred during Bronze Layer Pipeline: {e}")
        raise e
    
    finally:
        if spark is not None:
            spark.stop()
            print("Spark Session stopped.")

if __name__ == "__main__":
    main()