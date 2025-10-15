from pyspark.sql import SparkSession


# ===================== INIT SPARK =====================
def main():
    
    start_day="2023-01-01"
    end_day="2025-01-31"
    
    spark = (
        SparkSession.builder
        .appName("silver_to_gold_final")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    # Load and verify outputs
    gold_daily = spark.read.parquet(f"data/gold/gold_daily_platform_summary_{start_day}_{end_day}.parquet")
    gold_user = spark.read.parquet(f"data/gold/gold_user_snapshot_{start_day}_{end_day}.parquet")
    gold_post = spark.read.parquet(f"data/gold/gold_post_performance_{start_day}_{end_day}.parquet")
    gold_trend = spark.read.parquet(f"data/gold/gold_daily_content_trends_{start_day}_{end_day}.parquet")

    print("=== GOLD DAILY PLATFORM SUMMARY ===")
    gold_daily.show(5, truncate=False)

    print("\n=== GOLD USER SNAPSHOT ===")
    gold_user.select("user_id", "username", "total_posts_per_user", "account_age_days", "user_segment").show(5, truncate=False)

    print("\n=== GOLD POST PERFORMANCE ===")
    gold_post.select("post_id", "user_id", "post_sentiment", "total_likes", "total_comments").show(5, truncate=False)

    print("\n=== GOLD DAILY CONTENT TRENDS ===")
    gold_trend.select("report_date", "topic_or_keyword", "total_mentions", "trending_rank").show(10, truncate=False)