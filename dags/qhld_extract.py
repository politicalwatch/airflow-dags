import fnmatch
import os
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build

from datetime import datetime, timezone
from airflow import DAG
from airflow.models import Variable, TaskInstance
from airflow.utils.state import State
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.utils.dates import days_ago

SLACK_API_TOKEN = Variable.get("SLACK_API_TOKEN")
QHLD_DRIVE_FOLDER_ID = Variable.get("QHLD_DRIVE_FOLDER_ID")
RTVE_DRIVE_FOLDER_ID = Variable.get("RTVE_DRIVE_FOLDER_ID")


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


def keep_last_two_files(directory: str, pattern: str):
    files = sorted(
        (
            os.path.join(directory, filename)
            for filename in os.listdir(directory)
            if fnmatch.fnmatch(filename, pattern)
        ),
        key=os.path.basename,
    )

    # Delete all but the last two files
    for file in files[:-2]:
        os.remove(file)


def upload_to_drive(filepath, filename, folder_id):
    creds = Credentials.from_service_account_file("./keys/credentials.json")
    service = build("drive", "v3", credentials=creds)

    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(filepath, resumable=True)
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    print("File ID: %s" % file.get("id"))


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
    # on_success_callback=[
    #     send_slack_notification(
    #         text=":large_green_circle: Sin errores en procesamiento diario de datos de QHLD. \
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
    #         text=":red_circle: Hay errores en procesamiento diario de datos de QHLD.\
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

    extract_members = SSHOperator(
        task_id="extract_members",
        command="docker exec tipi-engine python quickex.py extractor members",
        ssh_hook=ssh,
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    update_groups = SSHOperator(
        task_id="update_groups",
        command="docker exec tipi-engine python quickex.py extractor calculate-composition-groups",
        ssh_hook=ssh,
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    extract_initiatives = SSHOperator(
        task_id="extract_initiatives",
        command="docker exec tipi-engine python quickex.py extractor initiatives",
        ssh_hook=ssh,
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    extract_votes = SSHOperator(
        task_id="extract_votes",
        command="docker exec tipi-engine python quickex.py extractor votes",
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

    tag = SSHOperator(
        task_id="tag",
        command="docker exec tipi-engine python quickex.py tagger all",
        ssh_hook=ssh,
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    alerts = SSHOperator(
        task_id="alerts",
        command="docker exec tipi-engine python quickex.py send-alerts",
        ssh_hook=ssh,
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    stats = SSHOperator(
        task_id="stats",
        command="docker exec tipi-engine python quickex.py stats",
        ssh_hook=ssh,
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
        text=":large_green_circle: Sin errores en procesamiento diario de datos de QHLD. \
        \n Running: {{ ti.xcom_pull(key='start_date') }} => {{ ti.xcom_pull(key='end_date') }} \
        \n Tiempo de ejecución: {{ ti.xcom_pull(key='duration') }}",
        dag=dag,
    )

    slack_end_failure = SlackAPIPostOperator(
        task_id="slack_end_failure",
        text=":red_circle: Hay errores en procesamiento diario de datos de QHLD.",
        dag=dag,
    )

    qhld_dump = SSHOperator(
        task_id="qhld_dump",
        ssh_hook=ssh,
        cmd_timeout=7200,
        command="""
            docker exec tipi-mongo  mongodump --username tipi --password tipi --db tipidb --archive > ~/backups/tipidb-daily-$(date +%Y-%m-%d).gz
        """,
        on_success_callback=[
            send_slack_notification(
                text=":large_green_circle: La tarea QHLD: {{ ti.task_id }} ha finalizado correctamente.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea QHLD: {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    qhld_copy_bkp = SFTPOperator(
        task_id="qhld_copy_bkp",
        ssh_hook=ssh,
        local_filepath="/home/airflow/backups/qhld/tipidb-daily-{{ ds }}.gz",
        remote_filepath="/home/ubuntu/backups/tipidb-daily-{{ ds }}.gz",
        operation="get",
        create_intermediate_dirs=True,
        # on_success_callback=[
        #     send_slack_notification(
        #         text=":large_green_circle: La tarea QHLD: {{ ti.task_id }} ha finalizado correctamente.",
        #         channel="#tech",
        #         username="PW Notify",
        #         icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
        #     )
        # ],
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea QHLD: {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    qhld_to_drive = PythonOperator(
        task_id="qhld_to_drive",
        python_callable=upload_to_drive,
        op_kwargs={
            "filepath": "/home/airflow/backups/qhld/tipidb-daily-{{ ds }}.gz",
            "filename": "tipidb-daily-{{ ds }}.gz",
            "folder_id": QHLD_DRIVE_FOLDER_ID,
        },
        # on_success_callback=[
        #     send_slack_notification(
        #         text=":large_green_circle: La tarea QHLD: {{ ti.task_id }} ha finalizado correctamente.",
        #         channel="#tech",
        #         username="PW Notify",
        #         icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
        #     )
        # ],
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea QHLD: {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    qhld_delete_old = PythonOperator(
        task_id="qhld_delete_old",
        python_callable=keep_last_two_files,
        op_kwargs={
            "directory": "/home/airflow/backups/qhld",
            "pattern": "tipidb-daily-????-??-??.gz",
        },
        # on_success_callback=[
        #     send_slack_notification(
        #         text=":large_green_circle: La tarea QHLD: {{ ti.task_id }} ha finalizado correctamente.",
        #         channel="#tech",
        #         username="PW Notify",
        #         icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
        #     )
        # ],
        on_failure_callback=[
            send_slack_notification(
                text=":warning: La tarea QHLD: {{ ti.task_id }} ha fallado.",
                channel="#tech",
                username="PW Notify",
                icon_url="https://politicalwatch.es/images/icons/icon_192px.png",
            )
        ],
    )

    (
        xcom_metadata
        >> extract_members
        >> update_groups
        >> extract_initiatives
        >> extract_votes
        >> tag
        >> alerts
        >> stats
        >> branch
        >> [slack_end_success, slack_end_failure]
        >> qhld_dump
        >> qhld_copy_bkp
        >> qhld_to_drive
        >> qhld_delete_old
    )

if __name__ == "__main__":
    dag.cli()
