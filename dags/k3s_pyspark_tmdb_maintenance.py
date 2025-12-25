from datetime import datetime
import logging
import time

from airflow.sdk import DAG  # pylint: disable=no-name-in-module

logger = logging.getLogger(__name__)


def k3s_pyspark_tmdb(conf: dict) -> None:
    from kubernetes import client, config

    config.load_incluster_config()
    batch = client.BatchV1Api()

    name = conf["metadata"]["name"]
    namespace = conf["metadata"].get("namespace", "spark")

    try:
        # Check if job exists
        batch.read_namespaced_job(name=name, namespace=namespace)

        # If exists, delete first (safe for immutable fields)
        batch.delete_namespaced_job(
            name=name,
            namespace=namespace,
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        )

        # Wait until job disappears (optional but safer)
        # Wait until job disappears (optional but safer)

        for _ in range(30):
            try:
                batch.read_namespaced_job(name=name, namespace=namespace)
                time.sleep(1)
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    break
                raise

        # Recreate job fresh
        batch.create_namespaced_job(namespace=namespace, body=conf)
        logger.info("Recreated Job: %s", name)

    except client.exceptions.ApiException as e:
        if e.status == 404:
            # Job does not exist yet → create new one
            batch.create_namespaced_job(namespace=namespace, body=conf)
            logger.info("Created Job: %s", name)
        else:
            raise e


def deploy_pyspark_tmdb_job(
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
    job = json.loads(data["job"])
    refined = json.loads(data["refined"])

    for j in [job, refined]:
        k3s_pyspark_tmdb(conf=j)


with DAG(
    dag_id="k3s_pyspark_tmdb_maintenance",
    start_date=datetime(2025, 11, 22, 2),
    schedule="0 3 * * *",
    catchup=False,
    tags=["tmdb", "pyspark", "maintenance", "k3s"],
):
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator

    start = EmptyOperator(task_id="start_execution")
    end = EmptyOperator(task_id="end_execution")

    deploy_tmdb_job = PythonOperator(
        task_id="deploy_pyspark_tmdb_app",
        python_callable=deploy_pyspark_tmdb_job,
        op_kwargs={
            "openbao_addr": "{{ var.json.OPENBAO_SECRET.openbao_addr }}",
            "username": "{{ var.json.OPENBAO_SECRET.username }}",
            "password": "{{ var.json.OPENBAO_SECRET.password }}",
            "namespace": "{{ var.json.OPENBAO_SECRET.namespace }}",
        },
    )

    start >> deploy_tmdb_job >> end
