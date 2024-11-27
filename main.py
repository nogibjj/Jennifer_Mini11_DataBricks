from mylib.extract import extract_data
from mylib.transform_load import transform_data, load_data
from pyspark.sql import SparkSession

def run_pipeline():
    """Run the complete data pipeline"""
    try:
        # Initialize Spark
        spark = SparkSession.builder.getOrCreate()
        
        print("Starting ETL pipeline...")
        
        # Extract
        print("1. Extracting data...")
        raw_data_path = extract_data()
        
        if raw_data_path:
            raw_df = spark.read.format("delta").load(raw_data_path)
            
            # Transform
            print("2. Transforming data...")
            transformed_df = transform_data(raw_df)
            
            # Load
            if transformed_df is not None:
                print("3. Loading transformed data...")
                output_path = "/dbfs/FileStore/tables/processed_goose_data"
                success = load_data(transformed_df, output_path)
                
                if success:
                    print("Pipeline completed successfully!")
                else:
                    print("Pipeline failed during loading phase.")
            else:
                print("Pipeline failed during transformation phase.")
        else:
            print("Pipeline failed during extraction phase.")
            
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")

if __name__ == "__main__":
    run_pipeline()