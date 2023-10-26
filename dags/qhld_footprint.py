from typing import Optional

from airflow import DAG
from airflow.models import TaskInstance, Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
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
        print(
            "Task failure notification: missing SLACK_WEBHOOK_URL",
            context["task_instance"].task_id,
        )
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
    dag_id="qhld_footprint",
    start_date=days_ago(2),
    schedule_interval="0 12 * * 0",
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
        text="Empezando el cálculo semanal de la huella de QHLD.",
    )

    footprint_calc = SSHOperator(
        task_id="footprint_calc",
        command="docker exec tipi-engine python quickex.py footprint",
        ssh_hook=ssh,
    )

    slack_end = SlackAPIPostOperator(
        task_id="slack_end",
        text="Fin del procesamiento semanal de la huella en QHLD.",
    )

    # notify_error = SimpleHttpOperator(
    #     task_id="notify_error",
    #     headers={"Content-Type": "application/json"},
    #     http_conn_id="n8n_slack",
    #     endpoint="webhook/868e2659-d6bd-407e-aa75-6a8ed4ebbd4c",
    #     data=json.dumps({"message": "Error durante la ejecucion."}),
    #     trigger_rule="one_failed",
    # )

    slack_start >> footprint_calc >> slack_end

if __name__ == "__main__":
    dag.cli()
