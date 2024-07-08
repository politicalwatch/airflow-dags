import json

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="congreso2030py_extract",
    start_date=days_ago(2),
    schedule_interval='@daily',
    catchup=False,
    tags=['pro', 'congreso2030py']
) as dag:

    ssh = SSHHook(ssh_conn_id='congreso2030py', key_file='./keys/pw_airflow')

    slack_start = SimpleHttpOperator(
        task_id='slack_start',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Empezando el procesamiento diario de datos de Congreso2030py.'})
    )

    extract_members = SSHOperator(
        task_id="extract_members",
        command="docker exec tipi-engine python quickex.py extractor members",
        ssh_hook=ssh
    )

    extract_initiatives = SSHOperator(
        task_id="extract_initiatives",
        command="docker exec tipi-engine python quickex.py extractor initiatives",
        ssh_hook=ssh
    )

    tag = SSHOperator(
        task_id="tag",
        command="docker exec tipi-engine python quickex.py tagger all",
        ssh_hook=ssh
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
        data=json.dumps({'message': 'Fin del procesamiento diario de datos de Congreso2030py.'})
    )

    notify_error = SimpleHttpOperator(
        task_id='notify_error',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Error durante la ejecucion.'}),
        trigger_rule='one_failed'
    )

    extract_members >> extract_initiatives >> tag >> stats

if __name__ == "__main__":
    dag.cli()
