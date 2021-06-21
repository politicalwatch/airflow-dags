from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="tipi_extract_local",
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['local', 'tipi']
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command="echo 'Hi'"
    )

    extract = BashOperator(
        task_id="extract",
        bash_command="docker exec -ti tipi-engine python quickex.py extractor initiatives"
    )

    tag = BashOperator(
        task_id="tag",
        bash_command="docker exec -ti tipi-engine python quickex.py tagger all"
    )

    start >> extract >> tag

if __name__ == "__main__":
    dag.cli()
