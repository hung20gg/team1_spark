## Setup Instructions for Airflow Docker Environment

Setup the variables for your Airflow environment.

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

```

Build the docker image and start the containers.

```bash
docker compose build
docker compose up -d
```


Hiện tại đang để tất cả là PythonOperator, tuy nhiên mỗi Operator đó thì lại là 1 spark instance ??? Hơi lạ đời nên phải verify lại