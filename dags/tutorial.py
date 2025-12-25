"""
Tutorial DAG from Airflow documentation.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "taskflow"],
)
def tutorial_dag():
    """
    This is a simple Airflow 3 DAG demonstrating the TaskFlow API.
    """

    @task
    def extract_data():
        """
        Simulates extracting data.
        """
        print("Extracting data...")
        return {"data": "some_raw_data"}

    @task
    def transform_data(raw_data: dict):
        """
        Simulates transforming data.
        """
        transformed = raw_data["data"].upper()
        print(f"Transforming data: {transformed}")
        return {"processed_data": transformed}

    @task
    def load_data(processed_data: dict):
        """
        Simulates loading data.
        """
        print(f"Loading data: {processed_data['processed_data']}")
        print("Data loaded successfully!")

    # Define the task dependencies
    extracted_info = extract_data()
    transformed_info = transform_data(raw_data=extracted_info)
    load_data(processed_data=transformed_info)


# Instantiate the DAG
tutorial_dag()
