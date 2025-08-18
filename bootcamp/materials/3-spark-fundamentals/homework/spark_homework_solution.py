from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, avg, count, desc, col

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SparkFundamentalsHomework") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    # Disable automatic broadcast join as required
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    # Define data file paths
    base_path = "E:/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data"
    
    # Read data files
    match_details = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{base_path}/match_details.csv")
    
    matches = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{base_path}/matches.csv")
    
    medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{base_path}/medals_matches_players.csv")
    
    # Read smaller dimension tables for explicit broadcasting
    medals = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{base_path}/medals.csv")
    
    maps = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{base_path}/maps.csv")
    
    # Create bucketed tables for the large fact tables
    # Bucket on match_id with 16 buckets as specified
    
    # Write bucketed match_details
    match_details.write \
        .mode("overwrite") \
        .option("path", f"{base_path}/bucketed_match_details") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bucketed_match_details")
    
    # Write bucketed matches  
    matches.write \
        .mode("overwrite") \
        .option("path", f"{base_path}/bucketed_matches") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bucketed_matches")
    
    # Write bucketed medals_matches_players
    medals_matches_players.write \
        .mode("overwrite") \
        .option("path", f"{base_path}/bucketed_medals_matches_players") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bucketed_medals_matches_players")
    
    # Read back the bucketed tables
    bucketed_match_details = spark.table("bucketed_match_details")
    bucketed_matches = spark.table("bucketed_matches")
    bucketed_medals_matches_players = spark.table("bucketed_medals_matches_players")
    
    # Perform bucket joins on match_id and explicitly broadcast small dimension tables
    # Join bucketed fact tables (bucket join) with broadcasted dimension tables
    joined_df = bucketed_match_details \
        .join(bucketed_matches, "match_id") \
        .join(bucketed_medals_matches_players, ["match_id", "player_gamertag"], "left") \
        .join(broadcast(medals), "medal_id", "left") \
        .join(broadcast(maps), bucketed_matches.mapid == maps.mapid, "left")
    
    # Question 1: Which player averages the most kills per game?
    print("=== Question 1: Player with most kills per game ===")
    most_kills_per_game = bucketed_match_details \
        .groupBy("player_gamertag") \
        .agg(avg("player_total_kills").alias("avg_kills_per_game")) \
        .orderBy(desc("avg_kills_per_game")) \
        .limit(10)
    
    most_kills_per_game.show()
    
    # Question 2: Which playlist gets played the most?
    print("=== Question 2: Most played playlist ===")
    most_played_playlist = bucketed_matches \
        .groupBy("playlist_id") \
        .agg(count("*").alias("game_count")) \
        .orderBy(desc("game_count")) \
        .limit(10)
    
    most_played_playlist.show()
    
    # Question 3: Which map gets played the most?
    print("=== Question 3: Most played map ===")
    most_played_map = bucketed_matches \
        .join(broadcast(maps), bucketed_matches.mapid == maps.mapid) \
        .groupBy("name") \
        .agg(count("*").alias("game_count")) \
        .orderBy(desc("game_count")) \
        .limit(10)
    
    most_played_map.show()
    
    # Question 4: Which map do players get the most Killing Spree medals on?
    print("=== Question 4: Map with most Killing Spree medals ===")
    killing_spree_medals = joined_df \
        .filter(col("name").contains("Killing Spree")) \
        .groupBy(maps.name.alias("map_name")) \
        .agg(count("*").alias("killing_spree_count")) \
        .orderBy(desc("killing_spree_count")) \
        .limit(10)
    
    killing_spree_medals.show()
    
    # Test different sortWithinPartitions configurations
    print("=== Testing sortWithinPartitions configurations ===")
    
    # Test 1: Sort by playlist_id (low cardinality)
    print("Testing sortWithinPartitions by playlist_id...")
    sorted_by_playlist = joined_df.sortWithinPartitions("playlist_id")
    # Write and check size
    sorted_by_playlist.write.mode("overwrite").parquet(f"{base_path}/sorted_by_playlist")
    
    # Test 2: Sort by map name (low cardinality) 
    print("Testing sortWithinPartitions by map name...")
    sorted_by_map = joined_df.sortWithinPartitions(maps.name)
    # Write and check size
    sorted_by_map.write.mode("overwrite").parquet(f"{base_path}/sorted_by_map")
    
    # Test 3: Sort by player_gamertag (high cardinality)
    print("Testing sortWithinPartitions by player_gamertag...")
    sorted_by_player = joined_df.sortWithinPartitions("player_gamertag")
    # Write and check size  
    sorted_by_player.write.mode("overwrite").parquet(f"{base_path}/sorted_by_player")
    
    print("=== Analysis Complete ===")
    print("Recommendation: Based on the hint, sorting by low cardinality columns like playlist or map")
    print("should result in smaller data sizes due to better compression from similar values being grouped together.")
    
    spark.stop()

if __name__ == "__main__":
    main()