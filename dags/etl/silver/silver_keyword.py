from pyspark.sql import SparkSession, functions as F
import random
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
import os
from dotenv import load_dotenv
load_dotenv()
# ============ INIT SPARK ============

current_dir = os.path.dirname(os.path.abspath(__file__))

import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def read_from_s3(spark, bucket, path):
    s3_path = f"s3a://{bucket}/{path}"
    df = spark.read.parquet(s3_path)
    return df

def save_to_s3(df, bucket, output_path, mode="overwrite"):
    s3_path = f"s3a://{bucket}/{output_path}"
    
    total_rows = df.count()
    target_rows_per_file = 100_000
    
    num_partitions = max(1, total_rows // target_rows_per_file)
    df = df.repartition(num_partitions)
    
    df.write.mode(mode).parquet(s3_path)
    logging.info(f"Saved dataframe to {s3_path} with mode={mode}")

def transform_silver_keyword(start_day, end_day):

    spark = (
        SparkSession.builder
        .appName("add_fake_sentiment_keywords")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate()
    )

    post_path = f"bronze/{start_day}_{end_day}/posts"
    comment_path = f"bronze/{start_day}_{end_day}/comments"

    # ============ LOAD SILVER PARQUETS ============
    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)

    # ============ ADD FAKE SENTIMENT ============
    # random sentiment (replace later with actual model inference)
    sentiments = ["positive", "neutral", "negative"]

    posts = posts.withColumn(
        "sentiment",
        F.when(F.rand() < 0.33, F.lit("positive"))
        .when(F.rand() < 0.66, F.lit("neutral"))
        .otherwise(F.lit("negative"))
    )

    comments = comments.withColumn(
        "sentiment",
        F.when(F.rand() < 0.33, F.lit("positive"))
        .when(F.rand() < 0.66, F.lit("neutral"))
        .otherwise(F.lit("negative"))
    )

    # ============ ADD FAKE KEYWORDS ============
    keywords_list = [
        ["ai", "machine learning", "innovation"],
        ["sports", "football", "health"],
        ["travel", "food", "culture"],
        ["technology", "mobile", "data"]
    ]

    # pick random keyword group for each record

    # Register UDF

    rand_keywords_udf = udf(lambda: random.choice(keywords_list), ArrayType(StringType()))
    logging.info("Registered UDF for random keywords.")

    posts = posts.withColumn("keywords", rand_keywords_udf())
    comments = comments.withColumn("keywords", rand_keywords_udf())

    # ============ SAVE BACK TO UPDATED PARQUETS ============
    update_post_dir = f"silver/{start_day}_{end_day}/posts"
    update_comment_dir = f"silver/{start_day}_{end_day}/comments"

    save_to_s3(posts, bucket="team1spark", output_path=update_post_dir, mode="overwrite")
    save_to_s3(comments, bucket="team1spark", output_path=update_comment_dir, mode="overwrite")

    logging.info("✅ Added fake sentiment and keywords columns to silver datasets.")
    spark.stop()
    
def main():
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    transform_silver_keyword(start_day, end_day)
    
if __name__ == "__main__":
    main()