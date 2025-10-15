from pyspark.sql import SparkSession, functions as F, Window
from datetime import date
from pyspark.sql.functions import broadcast

# ===================== INIT SPARK =====================
spark = (
    SparkSession.builder
    .appName("silver_to_gold_final")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

# ===================== LOAD SILVER PARQUETS =====================
posts = spark.read.parquet("silver_posts.parquet")
comments = spark.read.parquet("silver_comments.parquet")
likes = spark.read.parquet("likes.parquet")
users = spark.read.parquet("users.parquet")

# Ensure required columns exist
if "sentiment" not in posts.columns:
    posts = posts.withColumn("sentiment", F.lit("neutral"))
if "sentiment" not in comments.columns:
    comments = comments.withColumn("sentiment", F.lit("neutral"))

posts.cache()
comments.cache()
likes.cache()
users.cache()

# ============================================================
# 1️⃣ GOLD: DAILY PLATFORM SUMMARY
# ============================================================
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

daily_summary.repartition("report_date").write.mode("overwrite") \
    .partitionBy("report_date") \
    .option("compression", "snappy") \
    .parquet("gold_daily_platform_summary.parquet")

print("✅ gold_daily_platform_summary created")


# ============================================================
# 2️⃣ GOLD: USER SNAPSHOT
# ============================================================
user_activity = (
    users.alias("u")
    .join(
        posts.groupBy("user_id").agg(
            F.count("*").alias("total_posts_per_user"),
            F.mean(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_post_ratio"),
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
    F.mean(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_comment_received_ratio")
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
             F.mean("positive_comment_received_ratio").alias("positive_comment_received_ratio")
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

user_snapshot.repartition("snapshot_date").write.mode("overwrite") \
    .partitionBy("snapshot_date") \
    .option("compression", "snappy") \
    .parquet("gold_user_snapshot.parquet")

print("✅ gold_user_snapshot created")


# ============================================================
# 3️⃣ GOLD: POST PERFORMANCE
# ============================================================
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

post_perf.repartition("created_date").write.mode("overwrite") \
    .partitionBy("created_date") \
    .option("compression", "snappy") \
    .parquet("gold_post_performance.parquet")

print("✅ gold_post_performance created")


# ============================================================
# 4️⃣ GOLD: CONTENT TRENDS (Optional)
# ============================================================
if "keywords" in posts.columns:
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

print("✅ Silver → Gold transformation complete.")
