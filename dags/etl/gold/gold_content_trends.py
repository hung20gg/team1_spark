from pyspark.sql import SparkSession, functions as F, Window
import os
import sys

from dotenv import load_dotenv
load_dotenv()
# ============ INIT SPARK ============

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..', '..', '..'))

import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

from dags.etl.utils import read_from_s3, save_to_s3

def create_content_trends(start_day, end_day):

    spark = (
        SparkSession.builder
        .appName("build_content_trends")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate()
    )

    post_path = f"silver/{start_day}_{end_day}/posts"
    comment_path = f"silver/{start_day}_{end_day}/comments"

    # ============ LOAD BRONZE AND SILVER PARQUETS ============
    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)

    exploded_posts = posts.withColumn("topic_or_keyword", F.explode(F.col("keywords")))
    exploded_comments = comments.withColumn("topic_or_keyword", F.explode(F.col("keywords")))

    content_trends = (
        exploded_posts.groupBy(F.to_date("created_at").alias("report_date"), "topic_or_keyword")
        .agg(F.count("*").alias("mention_count_in_posts"))
        .join(
            exploded_comments.groupBy(F.to_date("created_at").alias("report_date"), "topic_or_keyword")
            .agg(F.count("*").alias("mention_count_in_comments")),
            ["report_date", "topic_or_keyword"],
            "outer"
        )
        .fillna(0)
        .withColumn("total_mentions", F.col("mention_count_in_posts") + F.col("mention_count_in_comments"))
        .withColumn("avg_sentiment_when_mentioned", F.lit(None).cast("float"))
        .withColumn("trending_rank",
                    F.row_number().over(Window.partitionBy("report_date").orderBy(F.desc("total_mentions"))))
    )
    content_trends.repartition("report_date").write.mode("overwrite") \
        .partitionBy("report_date") \
        .option("compression", "snappy") \
        .parquet("gold_daily_content_trends.parquet")

    
    # ============ SAVE BACK TO UPDATED PARQUETS ============
    content_trends_dir = f"gold/{start_day}_{end_day}/gold_daily_content_trends"


    save_to_s3(content_trends, bucket="team1spark", output_path=content_trends_dir, mode="overwrite")

    logging.info("✅ Created content trends gold dataset for {} to {}.".format(start_day, end_day))
    spark.stop()
    
def main():
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    create_daily_summary(start_day, end_day)
    
if __name__ == "__main__":
    main()