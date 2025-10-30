from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="cleanup_old_data",
    schedule_interval="0 20 * * 0",
    start_date=datetime(2020, 1, 1),
    catchup=False,
) as dag:
    cleanup = BashOperator(
        task_id="cleanup_old_runs",
        bash_command="airflow db cleanup --clean-before-timestamp $(date -d '30 days ago' +%Y-%m-%dT%H:%M:%S') --yes",
    )
