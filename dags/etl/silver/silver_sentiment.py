from pyspark.sql import SparkSession, functions as F
import random
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
import os
# ============ INIT SPARK ============

current_dir = os.path.dirname(os.path.abspath(__file__))

def transform_silver_sentiment(start_day, end_day):
    pass



def main():
    start_day="2025-01-01"
    end_day="2025-01-31"

    transform_silver_sentiment(start_day, end_day)

if __name__ == "__main__":
    main()