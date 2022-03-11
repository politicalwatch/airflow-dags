import json

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="qhld_footprint",
    start_date=days_ago(2),
    schedule_interval='0 12 * * 0',
    tags=['pro', 'qhld']
) as dag:

    ssh = SSHHook(ssh_conn_id='qhld', key_file='./keys/pw_airflow')

    slack_start = SimpleHttpOperator(
        task_id='slack_start',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Empezando el cálculo semanal de la huella de QHLD.'})
    )

    footprint_calc = SSHOperator(
        task_id="footprint_calc",
        command="docker exec tipi-engine python quickex.py footprint",
        ssh_hook=ssh
    )

    slack_end = SimpleHttpOperator(
        task_id='slack_end',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Fin del procesamiento semanal de la huella en QHLD.'})
    )

    notify_error = SimpleHttpOperator(
        task_id='notify_error',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Error durante la ejecucion.'}),
        trigger_rule='one_failed'
    )

    slack_start >> footprint_calc >> slack_end >> notify_error

if __name__ == "__main__":
    dag.cli()

