from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast
import os
import sys
from datetime import date


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

def create_user_snapshot(start_day, end_day):

    spark = initialize_spark(app_name="build_user_snapshot")

    post_path = f"silver/{start_day}_{end_day}/posts"
    comment_path = f"silver/{start_day}_{end_day}/comments"
    likes_path = f"silver/{start_day}_{end_day}/likes"
    users_path = f"silver/{start_day}_{end_day}/users"

    # ============ LOAD BRONZE AND SILVER PARQUETS ============
    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)
    likes = read_from_s3(spark, bucket="team1spark", path=likes_path)
    users = read_from_s3(spark, bucket="team1spark", path=users_path)

    user_activity = (
    users.alias("u")
    .join(
        posts.groupBy("user_id").agg(
            F.count("*").alias("total_posts_per_user"),
            F.mean(F.when(F.col("sentiment") == 1, 1).otherwise(0)).alias("positive_post_ratio"),
            F.sum(F.when(F.col("created_at") >= F.date_sub(F.current_date(), 30), 1).otherwise(0)).alias("posts_in_last_30_days")
        ), "user_id", "left"
    )
    .join(
        comments.groupBy("user_id").agg(
            F.count("*").alias("total_comments_given"),
            F.sum(F.when(F.col("created_at") >= F.date_sub(F.current_date(), 30), 1).otherwise(0)).alias("comments_in_last_30_days")
        ), "user_id", "left"
    )
    .join(
        likes.groupBy("user_id").agg(
            F.count("*").alias("total_likes_given")
        ), "user_id", "left"
    )
)

    # Received feedback
    comments_received = comments.groupBy("post_id").agg(
        F.count("*").alias("total_comments_received"),
        F.mean(F.when(F.col("sentiment") == 1, 1).otherwise(0)).alias("positive_comment_received_ratio")
    )
    likes_received = likes.groupBy("post_id").agg(F.count("*").alias("total_likes_received"))

    posts_feedback = (
        posts.join(broadcast(comments_received), "post_id", "left")
            .join(broadcast(likes_received), "post_id", "left")
            .groupBy("user_id")
            .agg(
                F.sum("total_comments_received").alias("total_comments_received"),
                F.sum("total_likes_received").alias("total_likes_received"),
                F.avg("total_likes_received").alias("avg_likes_per_post"),
                F.avg("total_comments_received").alias("avg_comments_per_post"),
                F.mean("positive_comment_received_ratio").alias("positive_comment_received_ratio"),
                F.count("total_comments_received").alias("total_feedback_count")
            )
    )

    # 🟩 Compute user's most recent activity date
    last_activity = (
        posts.select("user_id", "created_at")
        .union(comments.select("user_id", "created_at"))
        .union(likes.select("user_id", "created_at"))
        .groupBy("user_id")
        .agg(F.max("created_at").alias("last_active_date"))
    )

    # ✅ Build final user_snapshot (only once)
    user_snapshot = (
        user_activity.join(posts_feedback, "user_id", "left")
        .join(last_activity, "user_id", "left")
        .withColumn("account_age_days", F.datediff(F.current_date(), F.col("registration_date")))
        .fillna(0)
        .withColumn("snapshot_date", F.lit(date.today()))
    )

    # user_segment classification
    user_snapshot = user_snapshot.withColumn(
        "user_segment",
        F.when(F.col("account_age_days") <= 30, "New User")
        .when(F.col("last_active_date") < F.date_sub(F.current_date(), 90), "Churned")
        .when(F.col("last_active_date") < F.date_sub(F.current_date(), 30), "At Risk")
        .otherwise("Casual User")
    )

    # ============ SAVE BACK TO UPDATED PARQUETS ============
    user_snapshot_dir = f"gold/{start_day}_{end_day}/gold_user_snapshot"


    save_to_s3(user_snapshot, bucket="team1spark", output_path=user_snapshot_dir, mode="overwrite")

    logging.info("✅ Created user snapshot for {} to {}.".format(start_day, end_day))
    spark.stop()
    
def main():
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    create_user_snapshot(start_day, end_day)
    
if __name__ == "__main__":
    main()