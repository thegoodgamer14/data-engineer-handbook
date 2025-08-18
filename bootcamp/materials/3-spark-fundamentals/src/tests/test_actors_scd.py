import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import tempfile

from jobs.actors_scd_job import run_actors_scd


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("TestActorsSCD") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_actors_data(spark):
    """Create sample actors input data for testing"""
    
    # Define schema for actors
    schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actorid", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    # Sample data showing actor changes over time
    sample_data = [
        # Actor 1: quality class changes over time
        ("Tom Hanks", "actor1", 1990, "Good", True),
        ("Tom Hanks", "actor1", 1991, "Good", True),
        ("Tom Hanks", "actor1", 1992, "Great", True),  # Quality change
        ("Tom Hanks", "actor1", 1993, "Great", True),
        ("Tom Hanks", "actor1", 1994, "Great", False), # Active status change
        
        # Actor 2: starts inactive, becomes active, changes quality
        ("Julia Roberts", "actor2", 1988, "Average", False),
        ("Julia Roberts", "actor2", 1989, "Average", True),  # Active status change
        ("Julia Roberts", "actor2", 1990, "Good", True),     # Quality change
        ("Julia Roberts", "actor2", 1991, "Good", True),
        
        # Actor 3: single record (no changes)
        ("Morgan Freeman", "actor3", 1995, "Great", True)
    ]
    
    return spark.createDataFrame(sample_data, schema)


@pytest.fixture
def expected_scd_output(spark):
    """Create expected SCD output data for testing"""
    
    # Define schema for SCD output
    schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actorid", StringType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("start_date", IntegerType(), True),
        StructField("end_date", IntegerType(), True),
        StructField("current_flag", BooleanType(), True)
    ])
    
    # Expected SCD records based on the sample data
    expected_data = [
        # Actor 1 SCD records
        ("Tom Hanks", "actor1", "Good", True, 1990, 1991, False),      # 1990-1991: Good/Active
        ("Tom Hanks", "actor1", "Great", True, 1992, 1993, False),     # 1992-1993: Great/Active  
        ("Tom Hanks", "actor1", "Great", False, 1994, 9999, True),     # 1994-end: Great/Inactive
        
        # Actor 2 SCD records
        ("Julia Roberts", "actor2", "Average", False, 1988, 1988, False), # 1988: Average/Inactive
        ("Julia Roberts", "actor2", "Average", True, 1989, 1989, False),  # 1989: Average/Active
        ("Julia Roberts", "actor2", "Good", True, 1990, 9999, True),       # 1990-end: Good/Active
        
        # Actor 3 SCD record
        ("Morgan Freeman", "actor3", "Great", True, 1995, 9999, True)      # 1995-end: Great/Active
    ]
    
    return spark.createDataFrame(expected_data, schema)


def test_actors_scd_generation(spark, sample_actors_data, expected_scd_output):
    """Test the actors SCD job"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = f"{temp_dir}/input"
        output_path = f"{temp_dir}/output"
        
        # Write sample input data
        sample_actors_data.write.mode("overwrite").parquet(input_path)
        
        # Run the SCD job
        result_df = run_actors_scd(spark, input_path, output_path)
        
        # Read the output
        output_df = spark.read.parquet(output_path)
        
        # Sort both dataframes for consistent comparison
        result_sorted = result_df.orderBy("actorid", "start_date").collect()
        expected_sorted = expected_scd_output.orderBy("actorid", "start_date").collect()
        
        # Verify we have the same number of SCD records
        assert len(result_sorted) == len(expected_sorted)
        
        # Verify the schema
        assert set(result_df.columns) == set(expected_scd_output.columns)
        
        # Verify each SCD record
        for result_row, expected_row in zip(result_sorted, expected_sorted):
            assert result_row.actor == expected_row.actor
            assert result_row.actorid == expected_row.actorid
            assert result_row.quality_class == expected_row.quality_class
            assert result_row.is_active == expected_row.is_active
            assert result_row.start_date == expected_row.start_date
            assert result_row.end_date == expected_row.end_date
            assert result_row.current_flag == expected_row.current_flag


def test_single_actor_no_changes(spark):
    """Test SCD generation for a single actor with no changes"""
    
    # Define schema
    schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actorid", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    # Single actor with consistent attributes across multiple years
    single_actor_data = [
        ("Stable Actor", "stable1", 2000, "Good", True),
        ("Stable Actor", "stable1", 2001, "Good", True),
        ("Stable Actor", "stable1", 2002, "Good", True),
    ]
    
    input_df = spark.createDataFrame(single_actor_data, schema)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = f"{temp_dir}/input"
        output_path = f"{temp_dir}/output"
        
        # Write input data
        input_df.write.mode("overwrite").parquet(input_path)
        
        # Run the job
        result_df = run_actors_scd(spark, input_path, output_path)
        
        # Should only create one SCD record (the first year)
        assert result_df.count() == 1
        
        result_row = result_df.collect()[0]
        assert result_row.actor == "Stable Actor"
        assert result_row.start_date == 2000
        assert result_row.end_date == 9999
        assert result_row.current_flag == True


def test_empty_input(spark):
    """Test behavior with empty input"""
    
    # Create empty dataframe with correct schema
    schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actorid", StringType(), True), 
        StructField("year", IntegerType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = f"{temp_dir}/input"
        output_path = f"{temp_dir}/output"
        
        # Write empty input
        empty_df.write.mode("overwrite").parquet(input_path)
        
        # Run the job
        result_df = run_actors_scd(spark, input_path, output_path)
        
        # Should return empty dataframe with correct schema
        assert result_df.count() == 0
        expected_columns = {"actor", "actorid", "quality_class", "is_active", "start_date", "end_date", "current_flag"}
        assert set(result_df.columns) == expected_columns