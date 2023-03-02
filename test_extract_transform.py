from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'eakamon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='test_etl_dag_v03',
    default_args=default_args,
    start_date=datetime(2023, 3, 2),
    schedule_interval='@daily'
) as dag:
    task1 = KubernetesPodOperator(
        namespace="airflow-cluster",
        image="registry.thinknet.co.th/big-data/extract-from-s3:1.1.0",
        image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
        # cmds=["bash", "-cx"],
        # arguments=["echo", "10", "echo pwd"],
        is_delete_operator_pod=False,
        configmaps=["dev-configmap-accounting-tnanalytics"],
        labels={"foo": "bar"},
        name="test_etl_extract",
        in_cluster=True,
        task_id="task-one-extract",
        get_logs=True,
    )

    task2 = KubernetesPodOperator(
        namespace="airflow-cluster",
        image="registry.thinknet.co.th/big-data/transform:2.0.0",
        image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
        # cmds=["bash", "-cx"],
        # arguments=["echo", "10", "echo pwd"],
        is_delete_operator_pod=False,
        configmaps=["dev-configmap-accounting-tnanalytics"],
        labels={"foo": "bar"},
        name="test_etl_transform",
        in_cluster=True,
        task_id="task-two-transform",
        get_logs=True,
    )
    
    task1 >> task2