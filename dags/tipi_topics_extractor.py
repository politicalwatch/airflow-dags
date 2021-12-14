import json

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="tipi_topics_extractor",
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['topics', 'tipi']
) as dag:

    ssh_topics = SSHHook(ssh_conn_id='topics', key_file='./keys/pw_airflow')
    ssh = SSHHook(ssh_conn_id='tipi', key_file='./keys/pw_airflow')

    slack_start = SimpleHttpOperator(
        task_id='slack_start',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Empezando extración de topics de TIPI.'})
    )

    extract = SSHOperator(
        task_id="extract",
        command="docker exec topic-extract python app.py parlamento2030.json",
        ssh_hook=ssh_topics
    )

    slack_upload = SimpleHttpOperator(
        task_id='slack_upload',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Subiendo fichero de topics a TIPI.'})
    )

    copy = SSHOperator(
        task_id="copy",
        command="docker cp topic-extract:/app/topics.json ${AIRFLOW_HOME}/topics.json",
        ssh_hook=ssh_topics
    )

    upload = SSHOperator(
        task_id="upload",
        command="scp -i ${AIRFLOW_HOME}/keys/pw_airflow ${AIRFLOW_HOME}/topics.json p2030:topics.json",
        ssh_hook=ssh_topics
    )

    slack_import = SimpleHttpOperator(
        task_id='slack_import',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Importando topics.'})
    )

    copy_to_docker = SSHOperator(
        task_id="copy_to_docker",
        command="docker cp topics.json tipi-mongo:/tmp",
        ssh_hook=ssh
    )

    import_topics = SSHOperator(
        task_id="import_topics",
        command="docker exec tipi-mongo mongoimport -u tipi -p tipi -d tipidb -c topics --drop --jsonArray /tmp/topics.json",
        ssh_hook=ssh
    )

    slack_end = SimpleHttpOperator(
        task_id='slack_end',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Terminada la extraccion e importado de los topics.'})
    )

    notify_error = SimpleHttpOperator(
        task_id='notify_error',
        headers={"Content-Type": "application/json"},
        http_conn_id="n8n_slack",
        endpoint='webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c',
        data=json.dumps({'message': 'Error durante la ejecucion.'}),
        trigger_rule='one_failed'
    )

    slack_start >> extract >> slack_upload >> copy >> upload >> slack_import >> copy_to_docker >> import_topics >> slack_end >> notify_error

if __name__ == "__main__":
    dag.cli()
