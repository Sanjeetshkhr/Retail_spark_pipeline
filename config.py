"""
Configuration for Spark pipeline
"""

# Data Paths
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

# Spark configuration
SPARK_APP_NAME = "MedallionPipeline"
SPARK_MASTER = "local[*]"

# Dataset configuration
DATASET_URL = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/by-day/2010-12-01.csv"
DATASET_NAME = "retail_data"

# Logging
LOG_LEVEL = "WARN"