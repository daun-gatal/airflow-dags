"""
Example DAG demonstrating simple operators and dependencies.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="example_dag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
):
    EmptyOperator(task_id="task")
