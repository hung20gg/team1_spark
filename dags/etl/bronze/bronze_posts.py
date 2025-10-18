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

from dags.etl.bronze.data_cleaning import (
    DataCleaning,
    print_missing_report,
    print_duplicate_report,
    print_invalid_email_report
)

def transform_bronze_posts(start_day, end_day):
    
    data_cleaning = DataCleaning()
    df_post = data_cleaning.table(table_name="posts",
                                    column="post_id",
                                    start_day=start_day,
                                    end_day=end_day,
                                    lowerBound=1,
                                    upperBound=100000,
                                    numPartitions=8)
    df_post = data_cleaning.clean_text(df_post, text_col="content")
    df_post = data_cleaning.remove_invalid_rows(df_post)

    missing_report = data_cleaning.check_missing(df_post)
    logging.info("Finished checking missing values.")
    print_missing_report(missing_report)

    stats, df_post = data_cleaning.check_duplicate(df_post, subset_cols=["post_id"], drop=True)
    logging.info("Finished checking duplicate values.")
    print_duplicate_report(stats)

    # data_cleaning.save_to_parquet(df_post, output_path=f"{current_dir}/../../data/bronze/{start_day}_{end_day}/posts", mode="overwrite")

    # Save to S3
    logging.info(df_post.schema)
    data_cleaning.save_to_s3(df_post, bucket="team1spark", output_path=f"bronze/{start_day}_{end_day}/posts", mode="overwrite")

def main():
    
    start_day="2025-01-01"
    end_day="2025-01-31"
    
    transform_bronze_posts(start_day, end_day)

if __name__ == "__main__":
    main()