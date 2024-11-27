import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from mylib.extract import extract_data
from mylib.transform_load import transform_data, load_data

def run_tests():
    spark = SparkSession.builder.getOrCreate()
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("team", StringType(), True),
        StructField("goose_eggs", IntegerType(), True),
        StructField("broken_eggs", IntegerType(), True),
        StructField("mehs", IntegerType(), True),
        StructField("league_average_gpct", DoubleType(), True),
        StructField("replacement_gpct", DoubleType(), True),
        StructField("gwar", DoubleType(), True),
        StructField("ppf", IntegerType(), True),
    ])
    
    data = [
        ("Player1", 2020, "Team1", 10, 2, 1, 0.75, 0.65, 2.5, 100),
        ("Player2", 2020, "Team2", 8, 3, 2, 0.70, 0.60, 1.8, 95),
        ("Player1", 2020, "Team1", 10, 2, 1, 0.75, 0.65, 2.5, 100)
    ]
    
    sample_df = spark.createDataFrame(data, schema)

    # Test extract
    output_path = extract_data()
    assert output_path is not None
    assert output_path == "/dbfs/FileStore/tables/goose_data"
    print("Extract test passed")

    # Test transform
    transformed_df = transform_data(sample_df)
    assert transformed_df.count() == 2
    
    first_row = transformed_df.first()
    assert abs(first_row["success_rate"] - 0.727) < 0.01
    assert abs(first_row["performance_diff"] - 0.100) < 0.01
    assert abs(first_row["adjusted_gwar"] - 1.71) < 0.01
    print("Transform test passed")

    # Test load
    output_path = "/dbfs/FileStore/tables/processed_goose_data"
    success = load_data(transformed_df, output_path)
    assert success is True
    
    loaded_df = spark.read.format("delta").load(output_path)
    assert loaded_df.count() == transformed_df.count()
    print("Load test passed")

if __name__ == "__main__":
    run_tests()
    print("All tests passed successfully!")