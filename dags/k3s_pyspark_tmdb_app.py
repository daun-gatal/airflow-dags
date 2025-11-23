from datetime import datetime
import logging

from airflow.sdk import DAG


logger = logging.getLogger(__name__)


def k3s_pyspark_tmdb(conf: dict) -> None:
    from kubernetes import client, config, utils
    from kubernetes.client.exceptions import ApiException

    config.load_incluster_config()
    api_client = client.ApiClient()

    try:
        utils.create_from_dict(api_client, conf, namespace="spark")
    except ApiException as e:
        if e.status == 409:
            logger.warning(f"Resource already exists")
        else:
            raise e
    except Exception as e:
        raise e


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
    app = json.loads(data["app"])

    k3s_pyspark_tmdb(conf=app)


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
