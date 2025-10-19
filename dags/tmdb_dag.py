from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
import requests


def get_movie_ids(start_date: str, end_date: str, api_key: str, base_url: str):
    import random
    import time

    page = 1

    params = {
        "include_adult": "false",
        "include_video": "false",
        "language": "en-US",
        "page": page,
        "primary_release_date.gte": start_date,
        "primary_release_date.lte": end_date,
    }

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    discover_url = base_url + "/discover/movie"
    details_url = base_url + "/movie/{movie_id}"

    def _get_movie_details(movie_id: int, details_url: str, headers: dict):
        response = requests.get(details_url.format(movie_id=movie_id), headers=headers)
        return response.json()

    while True:
        response = requests.get(
            discover_url.format(page=page), headers=headers, params=params
        )
        data = response.json()

        for movie in data.get("results", []):
            yield _get_movie_details(movie["id"], details_url, headers)
            time.sleep(random.randint(3, 5))  # To respect rate limits

        if page >= data.get("total_pages", 0):
            break

        page += 1


with DAG(
    dag_id="tmdb_dag",
    start_date=datetime(2025, 10, 19, 2),
    schedule="@daily",
    catchup=False,
    tags=["tmdb", "kafka", "producer"],
):
    start = EmptyOperator(task_id="start_execution")
    end = EmptyOperator(task_id="end_execution")

    produce_to_kafka = ProduceToTopicOperator(
        task_id="produce_tmdb_data_to_kafka",
        kafka_config_id="kafka_conn",
        topic="tmdb",
        producer_function="tmdb_dag.get_movie_ids",
        producer_function_kwargs={
            "api_key": "{{ var.value.TMDB_API_KEY }}",
            "base_url": "{{ var.value.TMDB_API_BASE_URL}}",
            "start_date": "{{ ds }}",
            "end_date": "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
        },
    )

    start >> produce_to_kafka >> end
