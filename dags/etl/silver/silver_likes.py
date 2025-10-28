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
    print_invalid_email_report
)

def transform_likes(start_day, end_day):

    data_cleaning = DataCleaning(run_name="silver_likes")
    df_like = data_cleaning.read_from_s3(bucket="team1spark", path=f"bronze/{start_day}_{end_day}/likes")

    missing_report = data_cleaning.check_missing(df_like)
    logging.info("Finished checking missing values.")
    print_missing_report(missing_report)

    stats, df_like = data_cleaning.check_duplicate(df_like, subset_cols=["user_id"], drop=True)
    logging.info("Finished checking duplicate values.")
    print_duplicate_report(stats)

    # data_cleaning.save_to_parquet(df_like, output_path=f"{current_dir}/../../data/bronze/{start_day}_{end_day}/likes", mode="overwrite")

    data_cleaning.save_to_s3(df_like, bucket="team1spark", output_path=f"silver/{start_day}_{end_day}/likes", mode="overwrite")
    
def main():

    start_day="2025-01-01"
    end_day="2025-01-31"
    
    transform_likes(start_day, end_day)

if __name__ == "__main__":
    main()