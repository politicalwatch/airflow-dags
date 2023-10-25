import json

import os
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.providers.slack.operators.slack_api import SlackAPIPostOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="qhld_extract",
    start_date=days_ago(2),
    schedule_interval='@daily',
    tags=['pro', 'qhld']
) as dag:

    ssh = SSHHook(ssh_conn_id='qhld', key_file='./keys/pw_airflow')
    
    slack_start = SlackAPIPostOperator(
        task_id='send_slack_message',
        token=os.environ.get('SLACK_API_TOKEN'),
        text='Empezando el procesamiento diario de datos de QHLD.',
        channel='#tech',
    )

    extract_members = SSHOperator(
        task_id="extract_members",
        command="docker exec tipi-engine python quickex.py extractor members",
        ssh_hook=ssh
    )

    update_groups = SSHOperator(
        task_id="update_groups",
        command="docker exec tipi-engine python quickex.py extractor calculate-composition-groups",
        ssh_hook=ssh
    )

    extract_initiatives = SSHOperator(
        task_id="extract_initiatives",
        command="docker exec tipi-engine python quickex.py extractor initiatives",
        ssh_hook=ssh
    )

    extract_votes = SSHOperator(
        task_id="extract_votes",
        command="docker exec tipi-engine python quickex.py extractor votes",
        ssh_hook=ssh
    )

    tag = SSHOperator(
        task_id="tag",
        command="docker exec tipi-engine python quickex.py tagger all",
        ssh_hook=ssh
    )

    alerts = SSHOperator(
        task_id="alerts",
        command="docker exec tipi-engine python quickex.py send-alerts",
        ssh_hook=ssh
    )

    stats = SSHOperator(
        task_id="stats",
        command="docker exec tipi-engine python quickex.py stats",
        ssh_hook=ssh
    )
    
    slack_end = SlackAPIPostOperator(
        task_id='send_slack_message',
        token=os.environ.get('SLACK_API_TOKEN'),
        text='Fin del procesamiento diario de datos de QHLD.',
        channel='#tech',
    )
    
    notify_error = SlackAPIPostOperator(
        task_id='send_slack_message',
        token=os.environ.get('SLACK_API_TOKEN'),
        text='Error durante la ejecucion (QHLD).',
        channel='#tech',
        trigger_rule='one_failed'
    )

    slack_start >> extract_members >> update_groups >> extract_initiatives >> extract_votes >> tag >> alerts >> stats >> slack_end >> notify_error

if __name__ == "__main__":
    dag.cli()

