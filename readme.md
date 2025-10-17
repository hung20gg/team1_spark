## Setup Instructions for Airflow Docker Environment

Setup the variables for your Airflow environment.

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Chech the `.env` file to ensure the `AIRFLOW_UID` and all necessary variables are set correctly.
The `.env` file should be under the same directory as your `docker-compose.yaml` file.

Build the docker image and start the containers.

```bash
docker compose build
docker compose up -d
```

Check
```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```



## Known Issues
- `mention_count_in_post` null in `gold_content_trends`: Need to verify the ETL logic for counting mentions in posts.
- `gold_post_performance` return only posts_content.


Hiện tại đang để tất cả là PythonOperator, tuy nhiên mỗi Operator đó thì lại là 1 spark instance ??? Hơi lạ đời nên phải verify lại