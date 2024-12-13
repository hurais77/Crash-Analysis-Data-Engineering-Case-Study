from pyspark.sql import SparkSession, DataFrame
from typing import Union
import yaml
from utils.logger import logger

def initialize_spark(app_name: str = "BCG_SparkCaseStudy") -> SparkSession:
    """
    Initializes Spark Session

    Args:
        app_name (str, optional): Name of the application. Defaults to "BCG_SparkCaseStudy".

    Returns:
        spark: Returns the Spark Session object
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}")
        raise

def load_csv_as_df(spark, path: str, format: str = "csv", header: bool = True) -> DataFrame:
    """
    Load csv from a given path and returns a dataframe.

    Args:
        spark (SparkSession): SparkSession Object
        path (str): Path to the file
        format (str, optional): Format of the file. Defaults to "csv".
        header (bool, optional): If header is specified in the data. Defaults to True.

    Returns:
        Dataframe: Returns the pyspark dataframe
    """
    try:
        df = spark.read.format(format).option("header", header).load(path)
        logger.info(f"CSV file loaded successfully from path: {path}")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file from path {path}: {e}")
        raise

def write_df_as_csv(df: DataFrame, path: str):
    """
    Writes the dataframe to a path as a CSV

    Args:
        df (DataFrame): Pyspark Dataframe
        path (str): Path where the file is to be written
    """
    try:
        df.coalesce(1).write.mode("overwrite").format('csv').option('header', 'true').save(path)
        logger.info(f"Dataframe written successfully to path: {path}")
    except Exception as e:
        logger.error(f"Error writing dataframe to path {path}: {e}")
        raise

def read_yaml_file(path: str) -> Union[dict, list]:
    """
    Reads a YAML configuration file and returns the configuration as a dictionary.

    Args:
        path (str): Path to the YAML configuration file.

    Returns:
        dict: Configuration data as a dictionary.
    """
    try:
        with open(path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"YAML configuration file read successfully from path: {path}")
        return config
    except FileNotFoundError as e:
        logger.error(f"Configuration file {path} not found: {e}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration file {path}: {e}")
        raise
