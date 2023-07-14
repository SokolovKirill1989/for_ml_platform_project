from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
default_args = {
                'owner': 'admin',
                'retry_delay': timedelta(minutes=5),
                'max_active_runs': 3
            }

dag = DAG(
    'dfg',
    default_args=default_args,
    description='submit spark-app as sparkApplication on kubernetes',
    schedule_interval='0 7 * * 1',
    start_date=datetime(2023,7,28,21,30),
    catchup=False,
)

t1 = SparkKubernetesOperator(
    task_id='spark_submit',
    namespace="dognauts-mtp",
    application_file="spark/dfg_spark_manifest.yaml",
    do_xcom_push=True,
    dag=dag,
)

t2 = SparkKubernetesSensor(
    task_id='spark_monitor',
    namespace="dognauts-mtp",
    application_name="{ task_instance.xcom_pull(task_ids='spark_submit')['metadata']['name'] }",
    dag=dag,
)
t1 >> t2