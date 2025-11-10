from datetime import datetime, timezone
from airflow import DAG
from airflow.models import Variable, TaskInstance
from airflow.utils.state import State
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

SLACK_API_TOKEN = Variable.get("SLACK_API_TOKEN")


def get_dag_metadata(**context):
    start_datetime = datetime.now(timezone.utc)
    context["ti"].xcom_push(key="start_datetime", value=start_datetime)


def check_previous_tasks(**context):
    dag = context["dag"]
    tasks = dag.tasks
    execution_date = context["execution_date"]
    start_date = context["ti"].xcom_pull(key="start_datetime")
    end_date = datetime.now(timezone.utc)
    difference = end_date - start_date
    total_seconds = difference.total_seconds()
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    duration = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))

    context["ti"].xcom_push(
        key="start_date", value=start_date.strftime("%Y-%m-%d %H:%M:%S")
    )
    context["ti"].xcom_push(
        key="end_date", value=end_date.strftime("%Y-%m-%d %H:%M:%S")
    )
    context["ti"].xcom_push(key="duration", value=duration)

    for task in tasks:
        if task.task_id == "slack_end_success" or task.task_id == "slack_end_failure":
            continue
        task_instance = TaskInstance(task, execution_date)
        if task_instance.current_state() == State.FAILED:
            return "slack_end_failure"
    return "slack_end_success"


with DAG(
    dag_id="qhld_footprint",
    start_date=datetime(2023, 10, 22),
    schedule_interval="0 12 * * 0",
    catchup=False,
    default_args={
        "slack_conn_id": "slack_api_default",
        "username": "PW Notify",
        "token": SLACK_API_TOKEN,
        "icon_url": "https://politicalwatch.es/images/icons/icon_192px.png",
        "channel": "#tech",
    },
    # on_success_callback=[
    #     send_slack_notification(
    #         text=":large_green_circle: Sin errores en procesamiento semanal de la huella en QHLD.\
    #         \n Start: {{ execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
    #         \n End: {{ next_execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
    #         \n Duration: {{ ((next_execution_date - execution_date).total_seconds() // 3600)|int }} horas {{ ((next_execution_date - execution_date).total_seconds() // 60 % 60)|int }} minutos {{ ((next_execution_date - execution_date).total_seconds() % 60)|int }} segundos.",
    #         channel="#tech",
    #         username="PW Notify",
    #         icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
    #     )
    # ],
    # on_failure_callback=[
    #     send_slack_notification(
    #         text=":red_circle: Hay errores en procesamiento semanal de la huella en QHLD.\
    #         \n Start: {{ execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
    #         \n End: {{ next_execution_date.strftime('%d-%m-%Y %H:%M:%S') }}.\
    #         \n Duration: {{ ((next_execution_date - execution_date).total_seconds() // 3600)|int }} horas {{ ((next_execution_date - execution_date).total_seconds() // 60 % 60)|int }} minutos {{ ((next_execution_date - execution_date).total_seconds() % 60)|int }} segundos.",
    #         channel="#tech",
    #         username="PW Notify",
    #         icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
    #     )
    # ],
    tags=["pro", "qhld"],
) as dag:
    ssh = SSHHook(ssh_conn_id="qhld", key_file="./keys/pw_airflow", cmd_timeout=7200)

    xcom_metadata = PythonOperator(
        task_id="get_dag_metadata",
        python_callable=get_dag_metadata,
        provide_context=True,
    )

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

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=check_previous_tasks,
        provide_context=True,
        dag=dag,
    )

    slack_end_success = SlackAPIPostOperator(
        task_id="slack_end_success",
        text=":large_green_circle: Sin errores en procesamiento semanal de la huella en QHLD. \
        \n Tiempo de ejecución: {{ ti.xcom_pull(key='duration') }}",
        dag=dag,
    )

    slack_end_failure = SlackAPIPostOperator(
        task_id="slack_end_failure",
        text=":red_circle: Hay errores en procesamiento semanal de la huella en QHLD.",
        dag=dag,
    )

    xcom_metadata >> footprint_calc >> branch >> [slack_end_success, slack_end_failure]

if __name__ == "__main__":
    dag.cli()
