import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunJobExecuteOperator,
)
from airflow.utils.dates import days_ago
from docker.types import Mount

default_args = {
    "owner": "duncnh",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "3.0_dbt_transformations",
    default_args=default_args,
    description="Trigger Cloud Run Job to run dbt transformations Docker container to build BigQuery tables",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    print_date = BashOperator(task_id="print_current_date", bash_command="date")

    trigger_cloud_run_job = CloudRunJobExecuteOperator(
        task_id="trigger_cloud_run_job",
        project_id="chi-traffic-gcp",
        location="us-central1",
        job_name="dbt-cloud-run-job",
    )

    print_end_task = BashOperator(
        task_id="echo_end_task",
        bash_command='echo "DBT runs finished successfully"',
    )

    print_date >> trigger_cloud_run_job >> print_end_task
