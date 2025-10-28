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

from dags.etl.silver.data_cleaning import (
    DataCleaning,
    print_missing_report,
    print_duplicate_report,
)



def transform_comments(start_day, end_day):
    
    path = f"bronze/{start_day}_{end_day}/comments"
    data_cleaning = DataCleaning(run_name="silver_comments")
    df_comment = data_cleaning.read_from_s3(bucket="team1spark", path=path)

    missing_report = data_cleaning.check_missing(df_comment)
    logging.info("Finished checking missing values.")
    print_missing_report(missing_report)

    stats, df_comment = data_cleaning.check_duplicate(df_comment, subset_cols=["comment_id"], drop=True)
    logging.info("Finished checking duplicate values.")
    print_duplicate_report(stats)

    # Save to parquet
    # data_cleaning.save_to_parquet(df_comment, output_path=f"{current_dir}/../../data/bronze/{start_day}_{end_day}/comments", mode="overwrite")
    
    # Save to S3
    data_cleaning.save_to_s3(df_comment, bucket="team1spark", output_path=f"silver/{start_day}_{end_day}/comments", mode="overwrite")
    
def main():
    
    start_day="2025-01-01"
    end_day="2025-01-31"
    transform_comments(start_day, end_day)

if __name__ == "__main__":
    main()