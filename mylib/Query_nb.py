# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, desc, year, 
    round, when, dense_rank, 
    window
)
from pyspark.sql.window import Window

def initialize_spark():
    """Initialize Spark session"""
    return SparkSession.builder.getOrCreate()

def load_delta_table(spark, table_path):
    """Load Delta table into DataFrame"""
    try:
        return spark.read.format("delta").load(table_path)
    except Exception as e:
        print(f"Error loading table: {str(e)}")
        return None

def analyze_performance_metrics(df):
    """Analyze key performance metrics"""
    print("\n=== Performance Metrics Analysis ===")
    
    # Calculate success rate (goose_eggs vs broken_eggs)
    performance_df = df.withColumn(
        "success_rate",
        round(col("goose_eggs") / (col("goose_eggs") + col("broken_eggs") + 0.001) * 100, 2)
    )
    
    print("\nTop Performers by Success Rate:")
    performance_df.filter(col("goose_eggs") > 0) \
        .select("name", "team", "league", "success_rate", "goose_eggs", "broken_eggs") \
        .orderBy(desc("success_rate")) \
        .show(5)

def league_analysis(df):
    """Analyze statistics by league"""
    print("\n=== League-based Analysis ===")
    
    league_stats = df.groupBy("league") \
        .agg(
            round(avg("league_average_gpct"), 3).alias("avg_league_gpct"),
            round(avg("replacement_gpct"), 3).alias("avg_replacement_gpct"),
            sum("goose_eggs").alias("total_goose_eggs"),
            sum("broken_eggs").alias("total_broken_eggs"),
            round(avg("gwar"), 3).alias("avg_gwar")
        )
    
    print("\nLeague Performance Comparison:")
    league_stats.show()

def player_rankings(df):
    """Generate player rankings based on various metrics"""
    print("\n=== Player Rankings ===")
    
    # Window for ranking
    window_spec = Window.orderBy(desc("gwar"))
    
    rankings_df = df.withColumn(
        "player_rank", 
        dense_rank().over(window_spec)
    ).select(
        "name", 
        "team", 
        "league",
        "gwar",
        "player_rank",
        "ppf"
    )
    
    print("\nTop Players by GWAR (Goose Wins Above Replacement):")
    rankings_df.filter(col("gwar").isNotNull()) \
        .orderBy("player_rank") \
        .show(5)

def team_analysis(df):
    """Analyze performance by team"""
    print("\n=== Team Analysis ===")
    
    team_stats = df.groupBy("team") \
        .agg(
            count("*").alias("num_players"),
            sum("goose_eggs").alias("total_goose_eggs"),
            sum("broken_eggs").alias("total_broken_eggs"),
            round(avg("gwar"), 3).alias("avg_gwar"),
            round(avg("ppf"), 2).alias("avg_ppf")
        )
    
    print("\nTeam Performance Summary:")
    team_stats.orderBy(desc("total_goose_eggs")).show()

def save_analysis_results(df, analysis_name):
    """Save analysis results as Delta table"""
    output_path = f"/dbfs/FileStore/tables/analysis_{analysis_name}"
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)
    print(f"Analysis results saved to: {output_path}")

def main():
    # Initialize Spark
    spark = initialize_spark()
    
    # Load processed data
    df = load_delta_table(spark, "/dbfs/FileStore/tables/processed_goose_data")
    
    if df is not None:
        # Run analyses
        analyze_performance_metrics(df)
        league_analysis(df)
        player_rankings(df)
        team_analysis(df)
        
        # Save specific analysis results
        best_performers = df.orderBy(desc("gwar")).limit(100)
        save_analysis_results(best_performers, "top_performers")
    else:
        print("No data available for analysis")

if __name__ == "__main__":
    main()

# COMMAND ----------

display(spark.read.format("delta").load("/dbfs/FileStore/tables/analysis_top_performers"))