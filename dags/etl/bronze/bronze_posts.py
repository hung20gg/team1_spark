import os
import sys
import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..', '..', '..'))

from dags.etl.bronze.data_processing import (
    DataProcessing
)

def migrate_bronze_posts(start_day, end_day):
    
    data_cleaning = DataProcessing()
    df_post = data_cleaning.table(table_name="posts",
                                    column="post_id",
                                    start_day=start_day,
                                    end_day=end_day,
                                    lowerBound=1,
                                    upperBound=100000,
                                    numPartitions=8)

    # Save to S3
    data_cleaning.save_to_s3(df_post, bucket="team1spark", output_path=f"bronze/{start_day}_{end_day}/posts", mode="overwrite")

def main():
    
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    migrate_bronze_posts(start_day, end_day)

if __name__ == "__main__":
    main()