from pyspark.sql import SparkSession, functions as F
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

def create_daily_summary(start_day, end_day):

    spark = (
        SparkSession.builder
        .appName("build_daily_summary")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate()
    )

    post_path = f"silver/{start_day}_{end_day}/posts"
    comment_path = f"silver/{start_day}_{end_day}/comments"
    likes_path = f"bronze/{start_day}_{end_day}/likes"

    # ============ LOAD BRONZE AND SILVER PARQUETS ============
    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)
    likes = read_from_s3(spark, bucket="team1spark", path=likes_path)

    posts_daily = posts.groupBy(F.to_date("created_at").alias("report_date")).agg(
    F.countDistinct("post_id").alias("total_posts"),
    F.countDistinct("user_id").alias("active_posters"),
    F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_posts_count"),
    F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_posts_count")
)

    comments_daily = comments.groupBy(F.to_date("created_at").alias("report_date")).agg(
        F.countDistinct("comment_id").alias("total_comments"),
        F.countDistinct("user_id").alias("active_commenters"),
        F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_comments_count"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_comments_count")
    )

    likes_daily = likes.groupBy(F.to_date("created_at").alias("report_date")).agg(
        F.countDistinct("like_id").alias("total_likes"),
        F.countDistinct("user_id").alias("active_likers")
    )

    daily_summary = (
        posts_daily.join(comments_daily, "report_date", "outer")
        .join(likes_daily, "report_date", "outer")
        .fillna(0)
        .withColumn("avg_comments_per_post",
                    F.when(F.col("total_posts") > 0, F.col("total_comments") / F.col("total_posts")).otherwise(0))
        .withColumn("avg_likes_per_post",
                    F.when(F.col("total_posts") > 0, F.col("total_likes") / F.col("total_posts")).otherwise(0))
    )

    # total_active_users: distinct union of user_ids per day
    user_union = (
        posts.select(F.to_date("created_at").alias("report_date"), F.col("user_id"))
        .union(comments.select(F.to_date("created_at").alias("report_date"), F.col("user_id")))
        .union(likes.select(F.to_date("created_at").alias("report_date"), F.col("user_id")))
        .distinct()
    )
    active_user_counts = user_union.groupBy("report_date").agg(F.countDistinct("user_id").alias("total_active_users"))

    daily_summary = daily_summary.join(active_user_counts, "report_date", "left").fillna(0)
    
    # ============ SAVE BACK TO UPDATED PARQUETS ============
    daily_summary_dir = f"gold/{start_day}_{end_day}/gold_daily_platform_summary"


    save_to_s3(daily_summary, bucket="team1spark", output_path=daily_summary_dir, mode="overwrite")

    logging.info("✅ Created daily summary gold dataset for {} to {}.".format(start_day, end_day))
    spark.stop()
    
def main():
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    create_daily_summary(start_day, end_day)
    
if __name__ == "__main__":
    main()