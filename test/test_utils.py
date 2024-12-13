import pytest, sys,os, tempfile
from pyspark.sql import SparkSession
sys.path.insert(0, '..')
from utils.utils import initialize_spark,load_csv_as_df, write_df_as_csv

@pytest.fixture(scope="module")
def spark():
    """
    Fixture to initialize Spark session for the tests.
    """
    return initialize_spark("TestApp")


def test_initialize_spark(spark):
    """
    Test SparkSession initialization.
    """
    # Verify that the Spark session is of type SparkSession
    assert isinstance(spark, SparkSession)
    assert spark.version is not None  # Check if Spark version is available


def test_load_csv_as_df(spark):
    """
    Test loading CSV file into DataFrame.
    """
    # Sample data for testing
    data = [("Alice", 29), ("Bob", 35), ("Charlie", 25)]
    columns = ["Name", "Age"]
    
    # Create a temporary directory using tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        # Path for the temporary CSV file
        input_csv_path = os.path.join(temp_dir, "test_input.csv")
        
        # Create a DataFrame
        input_df = spark.createDataFrame(data, columns)
        
        # Save the DataFrame as a CSV file (temporary file)
        input_df.coalesce(1).write.csv(input_csv_path, header=True, mode="overwrite")
        
        # Load the CSV file using the function to test
        df = load_csv_as_df(spark, input_csv_path)
        
        # Verify that the DataFrame loaded correctly
        assert df.count() == 3  # Ensure the correct number of rows
        assert set(df.columns) == set(columns)  # Ensure correct column names


def test_write_df_as_csv(spark):
    """
    Test writing DataFrame to CSV.
    """
    # Sample data for testing
    data = [("Alice", 29), ("Bob", 35), ("Charlie", 25)]
    columns = ["Name", "Age"]
    
    # Create a temporary directory using tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        # Path for the temporary CSV file
        output_csv_path = os.path.join(temp_dir, "test_output.csv")
        
        # Create a DataFrame
        input_df = spark.createDataFrame(data, columns)
        
        # Write the DataFrame to a CSV file
        write_df_as_csv(input_df, output_csv_path)
        
        # Check if the file is created
        output_files = os.listdir(output_csv_path)
        assert len(output_files) > 0  # Ensure that files were written in the temp directory
