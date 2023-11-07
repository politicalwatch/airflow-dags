from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

SLACK_API_TOKEN = Variable.get("SLACK_API_TOKEN")


with DAG(
    dag_id="qhld_footprint",
    start_date=datetime(2023, 10, 22),
    schedule_interval="0 12 * * 0",
    default_args={
        "slack_conn_id": "slack_api_default",
        "username": "PW Notify",
        "token": SLACK_API_TOKEN,
        "icon_url": "https://politicalwatch.es/images/icons/icon_192px.png",
        "channel": "#tech",
    },
    on_success_callback=[
        send_slack_notification(
            text=":large_green_circle: Sin errores en procesamiento semanal de la huella en QHLD.\
            \n Start: {{ execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
            \n End: {{ next_execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
            \n Duration: {{ ((next_execution_date - execution_date).total_seconds() // 3600)|int }} horas {{ ((next_execution_date - execution_date).total_seconds() // 60 % 60)|int }} minutos {{ ((next_execution_date - execution_date).total_seconds() % 60)|int }} segundos.",
            channel="#tech",
            username="PW Notify",
            icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
        )
    ],
    on_failure_callback=[
        send_slack_notification(
            text=":red_circle: Hay errores en procesamiento semanal de la huella en QHLD.\
            \n Start: {{ execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
            \n End: {{ next_execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
            \n Duration: {{ ((next_execution_date - execution_date).total_seconds() // 3600)|int }} horas {{ ((next_execution_date - execution_date).total_seconds() // 60 % 60)|int }} minutos {{ ((next_execution_date - execution_date).total_seconds() % 60)|int }} segundos.",
            channel="#tech",
            username="PW Notify",
            icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
        )
    ],
    tags=["pro", "qhld"],
) as dag:
    ssh = SSHHook(ssh_conn_id="qhld", key_file="./keys/pw_airflow", cmd_timeout=7200)

    footprint_calc = SSHOperator(
        task_id="footprint_calc",
        command="docker exec tipi-engine python quickex.py footprint",
        ssh_hook=ssh,
        cmd_timeout=7200,
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    footprint_calc

if __name__ == "__main__":
    dag.cli()
