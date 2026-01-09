from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Param
from datetime import datetime

with DAG(
    dag_id="ecommerce_reprocess_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={
        "year": Param(2011, type="integer"),
        "month": Param(7, type="integer"),
    },
    tags=["ecommerce", "reprocess"],
) as dag:

    reprocess = DockerOperator(
        task_id="reprocess_pipeline",
        image="ecommerce-etl:latest",
        command="""
        --mode reprocess
        --year {{ params.year }}
        --month {{ params.month }}
        """,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )
