import sys
sys.path.append("airflow")

from airflow.models import BaseOperator, DAG, TaskInstance
from datetime import datetime, timedelta
from operators.x_operator import XOperator
from os.path import join
from airflow.utils.dates import days_ago

with DAG(dag_id='x_DAG', start_date=days_ago(2), schedule_interval="@daily") as dag:

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"

    x_operator = XOperator(file_path=join("datalake/x_datascience",
                                            "extract_date={{ ds }}",
                                            "datascience_{{ ds_nodash }}.json"),
                                            query=query,
                                            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                            task_id='x_datascience')