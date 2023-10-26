from typing import Optional

from airflow import DAG
from airflow.models import TaskInstance, Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import days_ago

SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL")
SLACK_API_TOKEN = Variable.get("SLACK_API_TOKEN")


def task_failure_alert(context: dict):
    """Alert to slack channel on failed dag

    :param context: airflow context object
    """
    if not SLACK_WEBHOOK_URL:
        # Do nothing if slack webhook not set up
        return

    last_task: Optional[TaskInstance] = context.get("task_instance")
    dag_name = last_task.dag_id
    error_message = context.get("exception") or context.get("reason")
    execution_date = context.get("execution_date")
    dag_run = context.get("dag_run")
    task_instances = dag_run.get_task_instances()
    file_and_link_template = "<{log_url}|{name}>"
    failed_tis = [
        file_and_link_template.format(log_url=ti.log_url, name=ti.task_id)
        for ti in task_instances
        if ti.state == "failed"
    ]
    title = f":red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)}) failed tasks"
    msg_parts = {
        "Execution date": execution_date,
        "Failed Tasks": ", ".join(failed_tis),
        "Error": error_message,
    }
    msg = "\n".join(
        [title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]
    ).strip()

    SlackWebhookHook(
        webhook_token=SLACK_WEBHOOK_URL,
        message=msg,
    ).execute()


with DAG(
    dag_id="qhld_extract",
    start_date=days_ago(2),
    schedule_interval="@daily",
    default_args={
        "slack_conn_id": "slack_api_default",
        "username": "PW Notify",
        "token": SLACK_API_TOKEN,
        "icon_url": "https://politicalwatch.es/images/icons/icon_192px.png",
        "channel": "#tech",
    },
    on_success_callback=None,
    on_failure_callback=task_failure_alert,
    tags=["pro", "qhld"],
) as dag:
    ssh = SSHHook(ssh_conn_id="qhld", key_file="./keys/pw_airflow")

    slack_start = SlackAPIPostOperator(
        task_id="slack_start",
        text="Empezando el procesamiento diario de datos de QHLD.",
    )

    extract_members = SSHOperator(
        task_id="extract_members",
        command="docker exec tipi-engine python quickex.py extractor members",
        ssh_hook=ssh,
    )

    update_groups = SSHOperator(
        task_id="update_groups",
        command="docker exec tipi-engine python quickex.py extractor calculate-composition-groups",
        ssh_hook=ssh,
    )

    extract_initiatives = SSHOperator(
        task_id="extract_initiatives",
        command="docker exec tipi-engine python quickex.py extractor initiatives",
        ssh_hook=ssh,
    )

    extract_votes = SSHOperator(
        task_id="extract_votes",
        command="docker exec tipi-engine python quickex.py extractor votes",
        ssh_hook=ssh,
    )

    tag = SSHOperator(
        task_id="tag",
        command="docker exec tipi-engine python quickex.py tagger all",
        ssh_hook=ssh,
    )

    alerts = SSHOperator(
        task_id="alerts",
        command="docker exec tipi-engine python quickex.py send-alerts",
        ssh_hook=ssh,
    )

    stats = SSHOperator(
        task_id="stats",
        command="docker exec tipi-engine python quickex.py stats",
        ssh_hook=ssh,
    )

    slack_end = SlackAPIPostOperator(
        task_id="slack_end",
        text="Fin del procesamiento diario de datos de QHLD.",
    )

    # notify_error = SlackAPIPostOperator(
    #     task_id='notify_error',
    #     slack_conn_id="slack_api_default",
    #     text='Error durante la ejecucion (QHLD).',
    #     channel='#tech',
    #     trigger_rule='one_failed',
    #     is_active=False
    # )

    (
        slack_start
        >> extract_members
        >> update_groups
        >> extract_initiatives
        # >> extract_votes
        >> tag
        >> alerts
        >> stats
        >> slack_end
    )

if __name__ == "__main__":
    dag.cli()
