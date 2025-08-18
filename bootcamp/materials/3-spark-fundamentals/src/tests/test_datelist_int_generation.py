import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, MapType, ArrayType, IntegerType
from pyspark.sql.functions import col
from datetime import date
import tempfile
import shutil

from jobs.datelist_int_generation_job import run_datelist_int_generation


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("TestDatelistIntGeneration") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_input_data(spark):
    """Create sample input data for testing"""
    
    # Define schema for user_devices_cumulated
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("device_activity_datelist", MapType(
            StringType(),  # device type
            ArrayType(DateType())  # array of dates
        ), True),
        StructField("date", DateType(), True)
    ])
    
    # Sample data
    sample_data = [
        (
            "user1", 
            {
                "mobile": [date(2023, 1, 1), date(2023, 1, 2)],
                "desktop": [date(2023, 1, 1)]
            },
            date(2023, 1, 2)
        ),
        (
            "user2",
            {
                "mobile": [date(2023, 1, 3)],
                "tablet": [date(2023, 1, 1), date(2023, 1, 3)]
            },
            date(2023, 1, 3)
        )
    ]
    
    return spark.createDataFrame(sample_data, schema)


@pytest.fixture  
def expected_output_data(spark):
    """Create expected output data for testing"""
    
    # Define schema for expected output
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("device_activity_datelist_int", MapType(
            StringType(),  # device type
            ArrayType(IntegerType())  # array of date integers
        ), True),
        StructField("date", DateType(), True)
    ])
    
    # Expected data with dates converted to integers (yyyyMMdd format)
    expected_data = [
        (
            "user1",
            {
                "mobile": [20230101, 20230102],
                "desktop": [20230101]
            },
            date(2023, 1, 2)
        ),
        (
            "user2", 
            {
                "mobile": [20230103],
                "tablet": [20230101, 20230103]
            },
            date(2023, 1, 3)
        )
    ]
    
    return spark.createDataFrame(expected_data, schema)


def test_datelist_int_generation(spark, sample_input_data, expected_output_data):
    """Test the datelist int generation job"""
    
    # Create temporary directories for input and output
    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = f"{temp_dir}/input"
        output_path = f"{temp_dir}/output"
        
        # Write sample input data
        sample_input_data.write.mode("overwrite").parquet(input_path)
        
        # Run the job
        result_df = run_datelist_int_generation(spark, input_path, output_path)
        
        # Read the output
        output_df = spark.read.parquet(output_path)
        
        # Collect data for comparison
        result_data = result_df.collect()
        expected_data = expected_output_data.collect()
        
        # Verify we have the same number of rows
        assert len(result_data) == len(expected_data)
        
        # Verify the schema
        assert result_df.schema == expected_output_data.schema
        
        # Verify specific transformations
        for result_row, expected_row in zip(result_data, expected_data):
            assert result_row.user_id == expected_row.user_id
            assert result_row.date == expected_row.date
            
            # Check device activity datelist int conversion
            result_datelist = result_row.device_activity_datelist_int
            expected_datelist = expected_row.device_activity_datelist_int
            
            assert set(result_datelist.keys()) == set(expected_datelist.keys())
            
            for device_type in result_datelist.keys():
                result_dates = sorted(result_datelist[device_type])
                expected_dates = sorted(expected_datelist[device_type])
                assert result_dates == expected_dates


def test_empty_input(spark):
    """Test behavior with empty input"""
    
    # Create empty dataframe with correct schema
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("device_activity_datelist", MapType(
            StringType(),
            ArrayType(DateType())
        ), True),
        StructField("date", DateType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = f"{temp_dir}/input"
        output_path = f"{temp_dir}/output"
        
        # Write empty input
        empty_df.write.mode("overwrite").parquet(input_path)
        
        # Run the job
        result_df = run_datelist_int_generation(spark, input_path, output_path)
        
        # Should return empty dataframe with correct schema
        assert result_df.count() == 0
        assert "device_activity_datelist_int" in result_df.columns