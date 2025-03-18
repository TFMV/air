from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import time

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def process_numeric_data(**context):
    """Simulate processing numeric data"""
    time.sleep(random.uniform(1, 3))
    return "Numeric data processed"


def process_text_data(**context):
    """Simulate processing text data"""
    time.sleep(random.uniform(1, 3))
    return "Text data processed"


def process_image_data(**context):
    """Simulate processing image data"""
    time.sleep(random.uniform(1, 3))
    return "Image data processed"


def aggregate_results(**context):
    """Aggregate results from all processing tasks"""
    time.sleep(1)
    return "All data processing completed"


with DAG(
    "task_groups_example",
    default_args=default_args,
    description="A DAG demonstrating Task Groups for parallel data processing",
    schedule_interval=None,
    catchup=False,
    tags=["example", "task_groups"],
) as dag:

    # Create task groups for different data types
    with TaskGroup(group_id="data_processing") as data_processing:
        numeric_task = PythonOperator(
            task_id="process_numeric",
            python_callable=process_numeric_data,
            provide_context=True,
        )

        text_task = PythonOperator(
            task_id="process_text",
            python_callable=process_text_data,
            provide_context=True,
        )

        image_task = PythonOperator(
            task_id="process_image",
            python_callable=process_image_data,
            provide_context=True,
        )

    # Final aggregation task
    aggregate = PythonOperator(
        task_id="aggregate_results",
        python_callable=aggregate_results,
        provide_context=True,
    )

    # Set dependencies
    data_processing >> aggregate
