from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,  # Set catchup to False
}

dag = DAG(
    'gcp_dataproc_spark_job',
    default_args=default_args,
    description='A DAG to run Spark job on Dataproc',
    schedule_interval=timedelta(days=1),  # Schedule the DAG to run once a day
    start_date=days_ago(1),
    tags=['example'],
)

# Define cluster config
CLUSTER_NAME = 'airflow-cluster'
PROJECT_ID = 'opportune-lore-412209'
REGION = 'us-central1'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.1-debian11'
    }
}

# Add GCSObjectExistenceSensor task
file_sensor_task = GCSObjectExistenceSensor(
    task_id='file_sensor_task',
    bucket='airflow_assmt1',  # Replace with your GCS bucket name
    object='input_files/employee.csv',  # Replace with your daily CSV file path
    poke_interval=300,  # Poke every 5 mins
    timeout=43200,  # Maximum poke duration of 12 hours
    mode='poke',
    dag=dag,
)

create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

pyspark_job = {
    'main_python_file_uri': 'gs://airflow_assmt1/python_file/employee_batch.py'
}

submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job['main_python_file_uri'],
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',
    dag=dag,
)

# Set task dependencies
file_sensor_task >> create_cluster >> submit_pyspark_job >> delete_cluster