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
    
