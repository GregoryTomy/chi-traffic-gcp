from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import datetime

default_args = {
    "owner": "Composer Example 2",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": days_ago(1),
}

# Define the DAG object at the module level
dag = DAG(
    "composer_sample_dag_2",
    catchup=False,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
)

# Define the task and assign it to the DAG
print_dag_run_conf = BashOperator(
    task_id="print_dag_run_conf",
    bash_command="echo {{ dag_run.id }}",
    dag=dag,  # Ensure the task is attached to the dag object
)
