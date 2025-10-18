from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import lower, regexp_replace, trim
from pyspark.sql.functions import col, length

import os
import re
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def print_missing_report(missing_report):
    logging.info("Missing Values Report:")
    for item in missing_report:
        logging.info(f"Column: {item['column']}, Missing Count: {item['missing_count']}, Missing Ratio: {item['missing_ratio']:.4%}")


def print_duplicate_report(duplicate_report):
    logging.info("Duplicate Values Report:")
    logging.info(f"Duplicate Count: {duplicate_report['duplicate_count']}, Duplicate Ratio: {duplicate_report['duplicate_ratio']:.4%}")
    
    
def print_invalid_email_report(invalid_email_report):
    logging.info("Invalid Email Report:")
    logging.info(f"Valid Email Count: {invalid_email_report['valid_email_count']}, Invalid Email Count: {invalid_email_report['invalid_email_count']}, Invalid Email Ratio: {invalid_email_report['invalid_email_ratio']:.4%}")
    

class DataCleaning:
    def __init__(self, run_name="bronze_etl"):
        self.run_name = run_name
        self.spark = self.get_spark_session()
        self.jdbc_url, self.db_properties = self.get_db()


    def get_spark_session(self):
        spark = (
            SparkSession.builder
            .appName(self.run_name)
            .master("local[*]")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
            .config("spark.driver.memory", "8g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.4.1")
            .getOrCreate()
        )
        
        
        # Disable _SUCCESS file creation and checksum verification
        spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        spark._jsc.hadoopConfiguration().set("dfs.client.write.checksum", "false")

        return spark

    def get_db(self):
        db_connector = {
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD")
        }
        
        db_properties = {
            "user": db_connector["user"],
            "password": db_connector["password"],
            "driver": "org.postgresql.Driver"
        }
        jdbc_url = f"jdbc:postgresql://{db_connector['host']}:{db_connector['port']}/{db_connector['dbname']}"
        return jdbc_url, db_properties

    # ===================== EXTRACT FUNCTIONS =====================

    def table(self, table_name, column, start_day=None, end_day=None, lowerBound=1, upperBound=100000, numPartitions=8):
        query = table_name
        if start_day and end_day:
            try:
                temp_df = self.spark.read.jdbc(url=self.jdbc_url, table=table_name, properties=self.db_properties)
                if 'created_at' in temp_df.columns:
                    query = f"(SELECT * FROM {table_name} WHERE created_at BETWEEN '{start_day}' AND '{end_day}') AS t"
                else:
                    logging.warning(f"Warning: 'created_at' column not found in table '{table_name}'. Skipping date filter.")
            except Exception as e:
                logging.error(f"Error checking table schema for '{table_name}': {e}")
                logging.info("Skipping date filter.")


        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            column=column,
            lowerBound=lowerBound,
            upperBound=upperBound,
            numPartitions=numPartitions,
            properties=self.db_properties
        )

    # ===================== TRANSFORM FUNCTIONS =====================

    def check_missing(self, df):
        df_sample = df.sample(withReplacement=False, fraction=0.01, seed=42).cache()
        try:
            total_count = df_sample.count()
            agg_exprs = [
                F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_sample.columns
            ]
            missing_df = df_sample.agg(*agg_exprs)

            results = []
            row = missing_df.first().asDict()
            for col, miss in row.items():
                results.append({
                    "column": col,
                    "missing_count": miss,
                    "missing_ratio": miss / total_count if total_count > 0 else 0
                })
            return results
        finally:
            df_sample.unpersist()

    def clean_text(self, df, text_col, new_col='text_clean'):
        df = (
            df.withColumn(new_col, lower(F.col(text_col)))
              .withColumn(new_col, regexp_replace(F.col(new_col), r"https?://\S+", ""))
              .withColumn(new_col, regexp_replace(F.col(new_col), r"[^\p{L}\p{N}\s]+", " "))
              .withColumn(new_col, trim(regexp_replace(F.col(new_col), r"\s+", " ")))
        )
        logging.info(f"Text cleaning applied on column: {text_col}, new column: {new_col}")
        return df
    
    def remove_invalid_rows(self, df):
        ''' remove rows that has less than 10 characters in content '''
        logging.info("Removing invalid rows...")   
        return df.filter(length(col("content")) >= 10)

    def check_duplicate(self, df, subset_cols, drop=False):
        df.cache()
        try:
            total_count = df.count()
            distinct_count = df.dropDuplicates(subset=subset_cols).count()
            dup_count = total_count - distinct_count
            dup_ratio = dup_count / total_count if total_count > 0 else 0

            stats = {
                "duplicate_count": dup_count,
                "duplicate_ratio": dup_ratio
            }

            if drop and dup_count > 0:
                cleaned_df = df.dropDuplicates(subset=subset_cols)
                logging.info(f"Found {dup_count} duplicate rows. Dropped duplicates. New total: {distinct_count}")
                return stats, cleaned_df
            else:
                logging.info(f"Duplicate rows: {dup_count} ({dup_ratio:.4%})")
                return stats, df
        finally:
            df.unpersist()


    def check_invalid_email(self, df, email_col="email", drop=False):
        df.cache()
        try:
            regex_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

            counts = df.agg(
                F.sum(F.when(F.col(email_col).rlike(regex_pattern), 1).otherwise(0)).alias("valid"),
                F.sum(F.when(~F.col(email_col).rlike(regex_pattern), 1).otherwise(0)).alias("invalid"),
                F.count("*").alias("total")
            ).collect()[0]

            valid = counts["valid"]
            invalid = counts["invalid"]
            total = counts["total"]
            invalid_ratio = invalid / total if total > 0 else 0

            stats = {
                "valid_email_count": valid,
                "invalid_email_count": invalid,
                "invalid_email_ratio": invalid_ratio
            }

            if drop and invalid > 0:
                cleaned_df = df.filter(F.col(email_col).rlike(regex_pattern))
                logging.info(f"Found {invalid} invalid emails ({invalid_ratio:.4%}). Dropped invalid emails. New total: {valid}")
                return stats, cleaned_df
            else:
                logging.info(f"Valid emails: {valid}, Invalid emails: {invalid} ({invalid_ratio:.4%})")
                return stats, df
        finally:
            df.unpersist()

    # ===================== LOAD FUNCTIONS =====================
    
    def save_to_s3(self, df, bucket: str, output_path: str, mode: str = "overwrite"):
        s3_path = f"s3a://{bucket}/{output_path}"
        
        total_rows = df.count()
        target_rows_per_file = 100_000
        
        num_partitions = max(1, total_rows // target_rows_per_file)
        df = df.repartition(num_partitions)
        
        df.write.mode(mode).parquet(s3_path)
        logging.info(f"Data saved to S3 at: {s3_path}")

    def save_to_parquet(self, df, output_path: str, mode: str = "overwrite"):
        absolute_path = os.path.abspath(output_path)
        dir_path = os.path.dirname(absolute_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            logging.info(f"Created directory: {dir_path}")

        total_rows = df.count()
        target_rows_per_file = 100_000
        
        num_partitions = max(1, total_rows // target_rows_per_file)
        df = df.repartition(num_partitions)
            
        df.write.mode(mode).parquet(output_path)
        logging.info(f"Data saved to Parquet at: {output_path}")