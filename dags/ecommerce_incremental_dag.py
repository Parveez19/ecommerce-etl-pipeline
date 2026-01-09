from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_incremental_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "incremental"],
) as dag:

    run_incremental = DockerOperator(
        task_id="run_incremental_pipeline",
        image="ecommerce-etl:latest",
        command="--mode incremental",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            # optionally override env vars here
        },
    )
