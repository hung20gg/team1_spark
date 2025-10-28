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

from dags.etl.bronze.data_processing import DataProcessing
    

def migrate_bronze_likes(start_day, end_day):

    data_cleaning = DataProcessing(run_name="bronze_likes")
    df_like = data_cleaning.table(table_name="likes",
                                    column="like_id",
                                    start_day=start_day,
                                    end_day=end_day,
                                    lowerBound=1,
                                    upperBound=100000,
                                    numPartitions=8)


    # data_cleaning.save_to_parquet(df_like, output_path=f"{current_dir}/../../data/bronze/{start_day}_{end_day}/likes", mode="overwrite")

    data_cleaning.save_to_s3(df_like, bucket="team1spark", output_path=f"bronze/{start_day}_{end_day}/likes", mode="overwrite")
    
def main():

    start_day="2025-01-01"
    end_day="2025-01-31"
    
    migrate_bronze_likes(start_day, end_day)

if __name__ == "__main__":
    main()