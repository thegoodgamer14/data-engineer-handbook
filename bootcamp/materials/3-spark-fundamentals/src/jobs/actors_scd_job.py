from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, coalesce, lit, when, isNull
from pyspark.sql.window import Window


def run_actors_scd(spark, input_path, output_path):
    """
    Create SCD (Slowly Changing Dimension) records for actors.
    
    Args:
        spark: SparkSession
        input_path: Path to input actors data
        output_path: Path to save actors_history_scd data
    """
    
    # Read the actors data
    actors_df = spark.read.parquet(input_path)
    
    # Define window for LAG/LEAD operations partitioned by actorid, ordered by year
    window_spec = Window.partitionBy("actorid").orderBy("year")
    
    # Step 1: Create actor_changes CTE equivalent
    actor_changes = actors_df.select(
        col("actor"),
        col("actorid"), 
        col("year"),
        col("quality_class"),
        col("is_active"),
        lag("quality_class", 1).over(window_spec).alias("prev_quality_class"),
        lag("is_active", 1).over(window_spec).alias("prev_is_active"),
        lead("year", 1).over(window_spec).alias("next_year")
    )
    
    # Step 2: Create SCD records - filter for changes and calculate start/end dates
    scd_records = actor_changes.select(
        col("actor"),
        col("actorid"),
        col("quality_class"),
        col("is_active"),
        col("year").alias("start_date"),
        coalesce(col("next_year") - 1, lit(9999)).alias("end_date"),
        when(col("next_year").isNull(), lit(True)).otherwise(lit(False)).alias("current_flag")
    ).filter(
        # Only include records where there was a change
        col("prev_quality_class").isNull() |  # First record for actor
        (col("prev_quality_class") != col("quality_class")) |  # Quality class changed
        (col("prev_is_active") != col("is_active")) |  # Active status changed
        (col("prev_is_active").isNull() & col("is_active").isNotNull())  # First active record
    )
    
    # Write the result
    scd_records.write.mode("overwrite").parquet(output_path)
    
    return scd_records


def main():
    """Main function for standalone execution"""
    spark = SparkSession.builder \
        .appName("ActorsSCD") \
        .getOrCreate()
    
    # Example paths - adjust as needed
    input_path = "data/actors"
    output_path = "data/actors_history_scd"
    
    result_df = run_actors_scd(spark, input_path, output_path)
    
    print("Actors SCD processing completed successfully!")
    print(f"Results written to: {output_path}")
    
    # Show sample results
    result_df.show(10, truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()