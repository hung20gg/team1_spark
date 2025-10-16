from collections import deque
import pandas as pd
from io import StringIO
from pyspark.sql.functions import col, lower, regexp_replace, trim
import os, re
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import os
import sys
from dotenv import load_dotenv
load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..', '..', '..'))


from dags.etl.utils import read_from_s3, save_to_s3
# ============ INIT SPARK ============

# TODO: @quoc-khanh @MinhLee - Fix the model to adapt the new data schema. Add cleaning steps for text processing more details

current_dir = os.path.dirname(os.path.abspath(__file__))

class Model_Inference:
    def __init__(self, spark, bucket="team1spark", pipeline_path="models/nb_pipeline"):
        self.spark = spark
        self.bucket = bucket
        self.pipeline_path = f"s3a://{bucket}/{pipeline_path}"
        self.nb_pipeline = self.load_pipeline()

    def load_pipeline(self):
        return PipelineModel.load(self.pipeline_path)

    def predict(self, df):
        df = (
            df.withColumn("text_clean", lower(col("texts")))
                  .withColumn("text_clean", regexp_replace(col("text_clean"), r"https?://\S+", ""))
                  .withColumn("text_clean", regexp_replace(col("text_clean"), r"[^\p{L}\p{N}\s]+", " "))
                  .withColumn("text_clean", trim(regexp_replace(col("text_clean"), r"\s+", " ")))
        )
        pred_new = self.nb_pipeline.transform(df)

        return pred_new


def transform_silver_sentiment(start_day, end_day):

    spark = (
        SparkSession.builder
        .appName("Spark_TextML")
        .master("local[*]")                # Dùng toàn bộ CPU có sẵn
        .config("spark.driver.memory", "8g")         # 8 GB cho driver (vừa đủ, tránh OOM)
        .config("spark.executor.memory", "2g")       # 2 GB cho executor (vì local mode, chỉ 1 executor)
        .config("spark.driver.maxResultSize", "2g")  # Giới hạn kết quả trả về driver
        .config("spark.sql.shuffle.partitions", "16") # Giảm số shuffle partitions để đỡ overhead
        .config("spark.default.parallelism", "8")    # Giới hạn song song ở mức hợp lý
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate()
    )
    
    inference = Model_Inference(spark, bucket="team1spark", pipeline_path="models/nb_pipeline")

    post_path = f"silver/{start_day}_{end_day}/posts"
    comment_path = f"silver/{start_day}_{end_day}/comments"

    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)

    posts = inference.predict(posts)
    comments = inference.predict(comments)

    update_post_dir = f"silver/{start_day}_{end_day}/posts"
    update_comment_dir = f"silver/{start_day}_{end_day}/comments"

    save_to_s3(posts, bucket="team1spark", output_path=update_post_dir, mode="overwrite")
    save_to_s3(comments, bucket="team1spark", output_path=update_comment_dir, mode="overwrite")


def main():
    start_day="2025-01-01"
    end_day="2025-01-31"

    transform_silver_sentiment(start_day, end_day)

if __name__ == "__main__":
    main()