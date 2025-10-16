from pyspark.sql import SparkSession

from .utils import read_from_s3, save_to_s3
# ===================== INIT SPARK =====================
def verify_gold_data(start_day, end_day):
    
    spark = (
        SparkSession.builder
        .appName("silver_to_gold_final")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    # Load and verify outputs
    gold_daily = read_from_s3(spark, bucket="team1spark", path = f"gold/{start_day}_{end_day}/gold_daily_platform_summary.parquet")
    gold_user = read_from_s3(spark, bucket="team1spark", path = f"gold/{start_day}_{end_day}/gold_user_snapshot.parquet")
    gold_post = read_from_s3(spark, bucket="team1spark", path = f"gold/{start_day}_{end_day}/gold_post_performance.parquet")
    gold_trend = read_from_s3(spark, bucket="team1spark", path = f"gold/{start_day}_{end_day}/gold_daily_content_trends.parquet")

    print("=== GOLD DAILY PLATFORM SUMMARY ===")
    gold_daily.show(5, truncate=False)

    print("\n=== GOLD USER SNAPSHOT ===")
    gold_user.select("user_id", "username", "total_posts_per_user", "account_age_days", "user_segment").show(5, truncate=False)

    print("\n=== GOLD POST PERFORMANCE ===")
    gold_post.select("post_id", "user_id", "post_sentiment", "total_likes", "total_comments").show(5, truncate=False)

    print("\n=== GOLD DAILY CONTENT TRENDS ===")
    gold_trend.select("report_date", "topic_or_keyword", "total_mentions", "trending_rank").show(10, truncate=False)
    
def main():
    
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    verify_gold_data(start_day, end_day)
    
if __name__ == "__main__":
    main()
    
