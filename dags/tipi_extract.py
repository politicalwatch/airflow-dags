import json

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="tipi_extract",
    start_date=days_ago(2),
    schedule_interval='@daily',
    tags=['pro', 'tipi']
) as dag:

    ssh = SSHHook(ssh_conn_id='tipi', key_file='./keys/pw_airflow')

    slack_start = SimpleHttpOperator(
        task_id='slack_start',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Empezando extración de iniciativas en TIPI.'})
    )

    extract = SSHOperator(
        task_id="extract",
        command="docker exec tipi-engine python quickex.py extractor initiatives",
        ssh_hook=ssh
    )

    slack_tag = SimpleHttpOperator(
        task_id='slack_tag',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Empezando taggeado de iniciativas en TIPI.'})
    )

    tag = SSHOperator(
        task_id="tag",
        command="docker exec tipi-engine python quickex.py tagger all",
        ssh_hook=ssh
    )

    slack_alerts = SimpleHttpOperator(
        task_id='slack_alerts',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Enviando alertas en TIPI.'})
    )

    alerts = SSHOperator(
        task_id="alerts",
        command="docker exec tipi-engine python quickex.py alerts",
        ssh_hook=ssh
    )

    slack_stats = SimpleHttpOperator(
        task_id='slack_stats',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Actualizando stats en TIPI.'})
    )

    stats = SSHOperator(
        task_id="stats",
        command="docker exec tipi-engine python quickex.py stats",
        ssh_hook=ssh
    )

    slack_end = SimpleHttpOperator(
        task_id='slack_end',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Finalizada extración y taggeado en TIPI.'})
    )

    slack_start >> extract >> slack_tag >> tag >> slack_alerts >> alerts >> slack_stats >> stats >> slack_end

if __name__ == "__main__":
    dag.cli()

