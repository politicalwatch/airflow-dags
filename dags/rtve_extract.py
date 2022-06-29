import json

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="rtve_extract",
    start_date=days_ago(2),
    schedule_interval='@daily',
    tags=['pro', 'rtve']
) as dag:

    ssh = SSHHook(ssh_conn_id='rtve', key_file='./keys/pw_airflow')

    slack_start = SimpleHttpOperator(
        task_id='slack_start',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Extrayendo nuevos subtítulos de RTVE.'})
    )

    extract_subtitles = SSHOperator(
        task_id="extract_subtitles",
        command="docker exec engine python command.py load-programs",
        ssh_hook=ssh
    )

    slack_end = SimpleHttpOperator(
        task_id='slack_end',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Fin del procesamiento de subtítulos de RTVE.'})
    )

    notify_error = SimpleHttpOperator(
        task_id='notify_error',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Error durante la ejecucion de RTVE.'}),
        trigger_rule='one_failed'
    )

    slack_start >> extract_subtitles >> slack_end >> notify_error

if __name__ == "__main__":
    dag.cli()


