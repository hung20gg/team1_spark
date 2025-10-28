from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import TimestampType

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



class DataProcessing:
    def __init__(self, run_name="bronze_etl"):
        self.run_name = run_name
        self.spark = self.get_spark_session()
        self.jdbc_url, self.db_properties = self.get_db()


    def get_spark_session(self):
        spark = (
            SparkSession.builder
            .appName(self.run_name)
            .master("local[2]")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
            .config("spark.driver.memory", "2g")
            # .config("spark.driver.maxResultSize", "2g")
            # .config("spark.sql.shuffle.partitions", "32")
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