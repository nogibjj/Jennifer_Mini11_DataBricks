from pyspark.sql import SparkSession
import requests
import tempfile

def extract_data():
    """Extract data from GitHub URL and load into Databricks"""
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

    # CHANGE: Enable schema auto-merge for Delta tables
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    # Source URL
    url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/goose/goose_rawdata.csv"
    
    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
            # Fetch data from URL
            response = requests.get(url)
            if response.status_code == 200:
                # Write content to temporary file
                temp_file.write(response.content)
                temp_file.flush()
                
                # Read CSV into Spark DataFrame
                df = spark.read.csv(
                    f"file:{temp_file.name}",
                    header=True,
                    inferSchema=True
                )
                
                # Save to DBFS as Delta table
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save("/dbfs/FileStore/tables/goose_data")
                
                print("Data successfully extracted and saved to Delta table")
                return df
            else:
                print(f"Failed to retrieve data. Status Code: {response.status_code}")
                return None
            
    except Exception as e:
        print(f"Error in data extraction: {str(e)}")
        return None

def transform_data(df):
    """Apply transformations to the data"""
    if df is not None:
        # Example transformations
        transformed_df = df.dropDuplicates() \
                          .na.fill(0) \
                          .cache()
        
        # CHANGE: Add missing columns with default values for schema alignment
        from pyspark.sql.functions import lit
        transformed_df = transformed_df.withColumn("success_rate", lit(None).cast("double")) \
                                       .withColumn("performance_diff", lit(None).cast("double")) \
                                       .withColumn("adjusted_gwar", lit(None).cast("double"))
        
        return transformed_df
    return None

def load_data(df):
    """Load the transformed data to final destination"""
    if df is not None:
        # Save as Delta table
        # CHANGE: Use mergeSchema to handle new columns added during transformation
        df.write \
           .format("delta") \
           .mode("overwrite") \
           .option("mergeSchema", "true") \
           .save("/dbfs/FileStore/tables/processed_goose_data")
        
        print("Data successfully loaded to final destination")

def main():
    # Extract
    raw_df = extract_data()
    
    # Transform
    transformed_df = transform_data(raw_df)
    
    # Load
    load_data(transformed_df)

if __name__ == "__main__":
    main()
