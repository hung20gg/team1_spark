from pyspark.sql import SparkSession

import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


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