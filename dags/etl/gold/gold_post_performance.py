from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast
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

from dags.etl.utils import read_from_s3, save_to_s3, initialize_spark

def create_post_performance(start_day, end_day):

    spark = initialize_spark(app_name="build_post_performance")

    post_path = f"silver/{start_day}_{end_day}/posts"
    comment_path = f"silver/{start_day}_{end_day}/comments"
    likes_path = f"bronze/{start_day}_{end_day}/likes"
    users_path = f"bronze/{start_day}_{end_day}/users"

    # ============ LOAD BRONZE AND SILVER PARQUETS ============
    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)
    likes = read_from_s3(spark, bucket="team1spark", path=likes_path)
    users = read_from_s3(spark, bucket="team1spark", path=users_path)

    first_like = likes.groupBy("post_id").agg(F.min("created_at").alias("first_like_time"))
    first_comment = comments.groupBy("post_id").agg(F.min("created_at").alias("first_comment_time"))

    post_perf = (
        posts
        .join(broadcast(users.select("user_id", "username")), "user_id", "left")
        .join(broadcast(first_like), "post_id", "left")
        .join(broadcast(first_comment), "post_id", "left")
        .join(broadcast(likes.groupBy("post_id").agg(F.count("*").alias("total_likes"))), "post_id", "left")
        .join(broadcast(comments.groupBy("post_id").agg(F.count("*").alias("total_comments"))), "post_id", "left")
        .withColumn("time_to_first_like_minutes",
                    F.round((F.unix_timestamp("first_like_time") - F.unix_timestamp("created_at")) / 60))
        .withColumn("time_to_first_comment_minutes",
                    F.round((F.unix_timestamp("first_comment_time") - F.unix_timestamp("created_at")) / 60))
        .select(
            "post_id",
            F.to_date("created_at").alias("created_date"),
            F.col("content").alias("post_content"),
            F.col("sentiment").alias("post_sentiment"),
            "user_id", "username",
            "total_likes", "total_comments",
            "time_to_first_like_minutes", "time_to_first_comment_minutes"
        )
    )
    
    # ============ SAVE BACK TO UPDATED PARQUETS ============
    post_perf_dir = f"gold/{start_day}_{end_day}/gold_post_performance"


    save_to_s3(post_perf, bucket="team1spark", output_path=post_perf_dir, mode="overwrite")

    logging.info("✅ Created post performance gold dataset for {} to {}.".format(start_day, end_day))
    spark.stop()
    
def main():
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    create_post_performance(start_day, end_day)
    
if __name__ == "__main__":
    main()