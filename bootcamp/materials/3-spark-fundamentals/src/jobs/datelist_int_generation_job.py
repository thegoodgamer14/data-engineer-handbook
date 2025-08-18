from pyspark.sql import SparkSession
from pyspark.sql.functions import col, transform, date_format, expr
from pyspark.sql.types import IntegerType


def run_datelist_int_generation(spark, input_path, output_path):
    """
    Convert device_activity_datelist to datelist_int format.
    
    Args:
        spark: SparkSession
        input_path: Path to input data (user_devices_cumulated)
        output_path: Path to save output data
    """
    
    # Read the user_devices_cumulated data
    user_devices_df = spark.read.parquet(input_path)
    
    # Convert device_activity_datelist to datelist_int using SQL expression
    # This is equivalent to the PostgreSQL query:
    # transform_values(device_activity_datelist, (k, v) -> transform(v, date_val -> CAST(date_format(date_val, 'yyyyMMdd') AS INT)))
    result_df = user_devices_df.select(
        col("user_id"),
        expr("""
            transform_values(
                device_activity_datelist,
                (k, v) -> transform(v, date_val -> CAST(date_format(date_val, 'yyyyMMdd') AS INT))
            ) as device_activity_datelist_int
        """),
        col("date")
    )
    
    # Write the result
    result_df.write.mode("overwrite").parquet(output_path)
    
    return result_df


def main():
    """Main function for standalone execution"""
    spark = SparkSession.builder \
        .appName("DatelistIntGeneration") \
        .getOrCreate()
    
    # Example paths - adjust as needed
    input_path = "data/user_devices_cumulated"
    output_path = "data/user_devices_datelist_int"
    
    result_df = run_datelist_int_generation(spark, input_path, output_path)
    
    print("Datelist int generation completed successfully!")
    print(f"Results written to: {output_path}")
    
    # Show sample results
    result_df.show(5, truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()