from datetime import datetime
import logging
import time

from airflow.sdk import DAG


logger = logging.getLogger(__name__)


def k3s_pyspark_tmdb(conf: dict) -> None:
    from kubernetes import client, config

    config.load_incluster_config()
    api_client = client.CustomObjectsApi()
    name = conf["metadata"]["name"]
    namespace = conf["metadata"]["namespace"]

    try:
        # Delete the existing SparkApplication
        try:
            api_client.delete_namespaced_custom_object(
                group="spark.apache.org",
                version="v1beta1",
                namespace=namespace,
                plural="sparkapplications",
                name=name,
            )
            logger.info(f"Deleting existing SparkApplication: {name}")

        except client.exceptions.ApiException as e:
            if e.status == 404:
                logger.info(
                    f"SparkApplication {name} does not exist. Creating new one."
                )
            else:
                raise e

        # Optional: wait a bit until deletion is completed
        time.sleep(5)

        # Create again (apply)
        api_client.create_namespaced_custom_object(
            group="spark.apache.org",
            version="v1beta1",
            namespace=namespace,
            plural="sparkapplications",
            body=conf,
        )
        logger.info(f"Applied SparkApplication: {name}")

    except Exception as e:
        logger.error(f"Error deploying SparkApplication: {e}")
        raise


def deploy_pyspark_tmdb_app(
    openbao_addr: str, username: str, password: str, namespace: str
) -> None:

    import json
    import hvac

    openbao_client = hvac.Client(url=openbao_addr, namespace=namespace)
    auth_response = openbao_client.auth.userpass.login(
        username=username, password=password
    )
    openbao_client.token = auth_response["auth"]["client_token"]

    secret = openbao_client.secrets.kv.v2.read_secret_version(
        mount_point="secrets",
        path="services/pyspark/app/tmdb/resources",  # your KV engine
    )

    data = secret["data"]["data"]
    app = data["app"]
    connect = data["connect"]

    for job in [app, connect]:
        k3s_pyspark_tmdb(conf=job)


with DAG(
    dag_id="k3s_pyspark_tmdb_app",
    start_date=datetime(2025, 11, 22, 2),
    schedule="0 2 * * *",
    catchup=False,
    tags=["tmdb", "pyspark", "consumer", "k3s"],
):
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator

    start = EmptyOperator(task_id="start_execution")
    end = EmptyOperator(task_id="end_execution")

    deploy_tmdb_app = PythonOperator(
        task_id="deploy_pyspark_tmdb_app",
        python_callable=deploy_pyspark_tmdb_app,
        op_kwargs={
            "openbao_addr": "{{ var.json.OPENBAO_SECRET.openbao_addr }}",
            "username": "{{ var.json.OPENBAO_SECRET.username }}",
            "password": "{{ var.json.OPENBAO_SECRET.password }}",
            "namespace": "{{ var.json.OPENBAO_SECRET.namespace }}",
        },
    )

    start >> deploy_tmdb_app >> end
