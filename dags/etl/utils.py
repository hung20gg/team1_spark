from pyspark.sql import SparkSession
import os
import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def initialize_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")

        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate()
    )
    return spark

def read_from_s3(spark: SparkSession, bucket: str, path: str):
    s3_path = f"s3a://{bucket}/{path}"
    df = spark.read.parquet(s3_path)
    return df

def save_to_s3(df, bucket: str, output_path: str, mode: str = "overwrite"):
    s3_path = f"s3a://{bucket}/{output_path}"
    
    total_rows = df.count()
    target_rows_per_file = 100_000
    
    num_partitions = max(1, total_rows // target_rows_per_file)
    df = df.repartition(num_partitions)
    
    df.write.mode(mode).parquet(s3_path)
    logging.info(f"Saved dataframe to {s3_path} with mode={mode}")
    
    
class Parameters:
    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date