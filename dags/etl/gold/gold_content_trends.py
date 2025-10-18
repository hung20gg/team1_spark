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

from dags.etl.utils import read_from_s3, save_to_s3, initialize_spark

def create_content_trends(start_day, end_day):

    spark = initialize_spark(app_name="build_content_trends")

    post_path = f"silver/{start_day}_{end_day}/posts"
    comment_path = f"silver/{start_day}_{end_day}/comments"

    # ============ LOAD BRONZE AND SILVER PARQUETS ============
    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)
    if "keywords" in posts.columns:
        exploded_posts = posts.withColumn("topic_or_keyword", F.explode(F.col("keywords")))
        exploded_comments = comments.withColumn("topic_or_keyword", F.explode(F.col("keywords")))

        posts_agg = (
            exploded_posts
            .groupBy(F.to_date("created_at").alias("report_date"), "topic_or_keyword")
            .agg(
                F.count("*").alias("mention_count_in_posts"),
                F.avg("sentiment").alias("avg_sentiment_in_posts")
            )
        )

        comments_agg = (
            exploded_comments
            .groupBy(F.to_date("created_at").alias("report_date"), "topic_or_keyword")
            .agg(
                F.count("*").alias("mention_count_in_comments"),
                F.avg("sentiment").alias("avg_sentiment_in_comments")
            )
        )

        content_trends = (
            posts_agg.join(comments_agg, ["report_date", "topic_or_keyword"], "outer")
            .fillna({"mention_count_in_posts": 0, "mention_count_in_comments": 0})
            .withColumn("total_mentions", F.col("mention_count_in_posts") + F.col("mention_count_in_comments"))
            .withColumn(
                "avg_sentiment_when_mentioned",
                F.when(
                    F.col("total_mentions") > 0,
                    (
                        F.coalesce(F.col("avg_sentiment_in_posts"), F.lit(0.0)) * F.col("mention_count_in_posts")
                        + F.coalesce(F.col("avg_sentiment_in_comments"), F.lit(0.0)) * F.col("mention_count_in_comments")
                    ) / F.col("total_mentions")
                ).otherwise(F.lit(None).cast("float"))
            )
            .withColumn("trending_rank", F.row_number().over(Window.partitionBy("report_date").orderBy(F.desc("total_mentions"))))
        )


        # ============ SAVE BACK TO UPDATED PARQUETS ============
        content_trends_dir = f"gold/{start_day}_{end_day}/gold_daily_content_trends"


        save_to_s3(content_trends, bucket="team1spark", output_path=content_trends_dir, mode="overwrite")

        logging.info("✅ Created content trends gold dataset for {} to {}.".format(start_day, end_day))
    else:
        logging.info("No 'keywords' column found in posts dataset. Skipping content trends creation.")
    spark.stop()
    
def main():
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    create_content_trends(start_day, end_day)
    
if __name__ == "__main__":
    main()