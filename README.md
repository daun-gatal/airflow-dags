# Airflow DAGs Repository

This repository serves as the source of truth for **Apache Airflow DAGs** running on Kubernetes.

## 🚀 Deployment Architecture

This project utilizes a **GitOps** approach. The contents of this repository are automatically synchronized to the Airflow scheduler and webserver pods running in the Kubernetes cluster.

*   **Mechanism**: Kubernetes Sidecar (git-sync)
*   **Target**: `/opt/airflow/dags` on the cluster
*   **Sync Interval**: 60s (Configurable)

## 📂 Contents

The repository hosts workflows for:

1.  **TMDB Data Ingestion**: Fetching movie data and publishing to Kafka (`tmdb_dag`).
2.  **Spark on K8s**: Managing `SparkApplication` resources via Airflow (`k3s_pyspark_tmdb_app`).
3.  **Maintenance**: Automated cluster cleanup and job rotation (`k3s_pyspark_tmdb_maintenance`).

## 🛠 Configuration

Sensitive configurations and connections are injected at runtime via **OpenBao** (Vault) and Kubernetes Secrets, ensuring no credentials are stored in this codebase.
