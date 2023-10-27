from airflow import DAG
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.utils.dates import days_ago

SLACK_API_TOKEN = Variable.get("SLACK_API_TOKEN")

with DAG(
    dag_id="rtve_extract",
    start_date=days_ago(2),
    schedule_interval="@daily",
    default_args={
        "slack_conn_id": "slack_api_default",
        "username": "PW Notify",
        "token": SLACK_API_TOKEN,
        "icon_url": "https://politicalwatch.es/images/icons/icon_192px.png",
        "channel": "#tech",
    },
    on_success_callback=[
        send_slack_notification(
            text=":green_circle: Sin errores en la extracción de subtítulos de RTVE.",
        )
    ],
    on_failure_callback=[
        send_slack_notification(
            text=":red_circle: Hay errores en la extracción de subtítulos de RTVE.",
        )
    ],
    tags=["pro", "rtve"],
) as dag:
    ssh = SSHHook(ssh_conn_id="rtve", key_file="./keys/pw_airflow", cmd_timeout=7200)

    slack_start = SlackAPIPostOperator(
        task_id="slack_start",
        text="Extrayendo nuevos subtítulos de RTVE.",
    )

    api_extract = SSHOperator(
        task_id="api_extract",
        command="docker exec engine python command.py api-extract",
        ssh_hook=ssh,
        cmd_timeout=7200,
    )

    calculate_stats = SSHOperator(
        task_id="calculate_stats",
        command="docker exec engine python command.py calculate-stats",
        ssh_hook=ssh,
        cmd_timeout=7200,
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea {{ ti.task_id }} ha fallado.",
            )
        ],
    )

    slack_end = SlackAPIPostOperator(
        task_id="slack_end",
        text="Fin del procesamiento de subtítulos de RTVE.",
    )

    slack_start >> api_extract >> calculate_stats >> slack_end

if __name__ == "__main__":
    dag.cli()
