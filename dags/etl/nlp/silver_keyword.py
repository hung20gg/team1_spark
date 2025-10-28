from pyspark.sql import SparkSession, functions as F
import random
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.sql.functions import lower, regexp_replace, trim
from pyspark.sql.functions import udf
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
from pyspark.sql.types import StringType

def clean_text(df, text_col, new_col='text_clean'):

    # cast to string and replace null or literal 'nan' with empty string
    text_cast = F.col(text_col).cast(StringType())
    safe_text = F.when(text_cast.isNull() | (F.lower(text_cast) == 'nan'), F.lit('')).otherwise(text_cast)

    df = (
        df.withColumn(new_col, lower(safe_text))
          .withColumn(new_col, regexp_replace(F.col(new_col), r"https?://\S+", ""))
          .withColumn(new_col, regexp_replace(F.col(new_col), r"[^\p{L}\p{N}\s]+", " "))
          .withColumn(new_col, trim(regexp_replace(F.col(new_col), r"\s+", " ")))
    )
    logging.info(f"Text cleaning applied on column: {text_col}, new column: {new_col}")
    return df

def transform_silver_keyword(start_day, end_day):

    spark = initialize_spark(app_name="transform_silver_keyword")

    post_path = f"silver/{start_day}_{end_day}/posts"
    comment_path = f"silver/{start_day}_{end_day}/comments"

    # ============ LOAD SILVER PARQUETS ============
    posts = read_from_s3(spark, bucket="team1spark", path=post_path)
    comments = read_from_s3(spark, bucket="team1spark", path=comment_path)

    # ============ ADD FAKE SENTIMENT ============
    # random sentiment (replace later with actual model inference)
    # sentiments = [1, 0, -1]

    # sentiment_udf = udf(lambda: random.choice(sentiments), IntegerType())
    # logging.info("Registered UDF for random sentiment.")

    # posts = posts.withColumn("sentiment", sentiment_udf())
    # comments = comments.withColumn("sentiment", sentiment_udf())

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

    posts = clean_text(posts, text_col="content", new_col="text_clean")
    comments = clean_text(comments, text_col="content", new_col="text_clean")

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