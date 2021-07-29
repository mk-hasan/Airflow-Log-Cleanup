"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""
import logging
import os
from datetime import datetime, timedelta

import airflow
from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# airflow-log-cleanup

START_DATE = airflow.utils.dates.days_ago(1)
try:
    BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER").rstrip("/")
except Exception as e:
    BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")

DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get(
    "airflow_log_cleanup__max_log_age_in_days", 15
)

ENABLE_DELETE = Variable.get(
    "airflow_log_cleanup__enable_delete", False
)
# logs cleared.
NUMBER_OF_WORKERS = 1
DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]


default_args = {
    'owner': "P-AMI",
    'depends_on_past': False,
    'email': ["ami@go-bbg.com"],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(year=2019, month=5, day=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'run_as_user': 'chal-tec-sem'
}

dag = DAG(
    "airflow_log_clean",
    default_args=default_args,
    schedule_interval= "5 5 30 * *",
    start_date=datetime(year=2019, month=5, day=1),
    template_searchpath="/home/chal-tec-sem/harmony/"
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False


for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    for dir_id, directory in enumerate(DIRECTORIES_TO_DELETE):

        log_cleanup_op = BashOperator(
            task_id='log_cleanup_worker_num_' +
            str(log_cleanup_id) + '_dir_' + str(dir_id),
            bash_command="./log-clean.sh",
            params={
                "directory": str(directory),
                "sleep_time": int(log_cleanup_id)*3,
                "max_log_days": int(DEFAULT_MAX_LOG_AGE_IN_DAYS),
                "enable_delete": str(ENABLE_DELETE)},
            dag=dag)
