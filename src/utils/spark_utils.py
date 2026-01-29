"""
Utility function for spark pipeline
"""

from pyspark.sql import SparkSession
import requests
import os
from config.config import SPARK_APP_NAME, SPARK_MASTER, LOG_LEVEL

def get_spark_session(app_name = SPARK_APP_NAME):
    """
    Creates and return a spark session

    Args:
        app_name: name of the spark application as defined in config 

    Returns:
        SparkSession object
    """

    spark = SparkSession.builder.appname(app_name).master(SPARK_MASTER).getOrCreate()

    spark.sparkContext.setLogLevel(LOG_LEVEL)
    return spark

def download_dataset(url, output_path):
    """
    Download dataset from url
    
    Args:
        url: URL to download the dataset
        output_path: Local path to save the dataset

    Returns:
        Path to the downloaded file
    """

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    if os.path.exists(output_path):
        print(f"File already exists at {output_path}. \nSkipping download.")
        return output_path
    
    print(f"Downloading dataset from {url} ")
    response = requests.get(url)

    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Dataset downloaded and saved to {output_path}")
        return output_path
    else:
        raise Exception(f"Failed to download dataset from {url}. Status code: {response.status_code}")
    
def get_version():
    """Read and return version information"""
    version_file = "VERSION"
    if os.path.exists(version_file):
        with open(version_file, 'r') as f:
            version_info = {}
            for line in f:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    version_info[key] = value
            return version_info
    return {}

