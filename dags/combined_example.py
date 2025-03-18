from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.models import Pool
from airflow.utils.session import provide_session
from datetime import datetime, timedelta
import time
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Pool configuration
DB_POOL = "database_pool"
MAX_DB_CONNECTIONS = 2  # Maximum number of concurrent database connections


def check_pool_exists(**context):
    """Check if the required pool exists"""
    from airflow.models import Pool
    from airflow.utils.session import provide_session

    @provide_session
    def _check_pool(session):
        pool = session.query(Pool).filter(Pool.pool == DB_POOL).first()
        if not pool:
            raise Exception(
                f"Pool '{DB_POOL}' does not exist. Please create it in the Airflow UI or via CLI:\n"
                f"airflow pools set {DB_POOL} {MAX_DB_CONNECTIONS} 'Limits concurrent database operations'"
            )
        logger.info(f"Pool '{DB_POOL}' exists with {pool.slots} slots")
        return f"Pool '{DB_POOL}' verified"

    return _check_pool()


def process_user_data(**context):
    """Process user data"""
    try:
        task_id = context["task_instance"].task_id
        logger.info(f"Starting user data processing for task: {task_id}")
        time.sleep(random.uniform(1, 2))
        logger.info(f"Completed user data processing for task: {task_id}")
        return "User data processed"
    except Exception as e:
        logger.error(f"Error processing user data: {str(e)}")
        raise


def process_order_data(**context):
    """Process order data"""
    try:
        task_id = context["task_instance"].task_id
        logger.info(f"Starting order data processing for task: {task_id}")
        time.sleep(random.uniform(1, 2))
        logger.info(f"Completed order data processing for task: {task_id}")
        return "Order data processed"
    except Exception as e:
        logger.error(f"Error processing order data: {str(e)}")
        raise


def process_inventory_data(**context):
    """Process inventory data"""
    try:
        task_id = context["task_instance"].task_id
        logger.info(f"Starting inventory data processing for task: {task_id}")
        time.sleep(random.uniform(1, 2))
        logger.info(f"Completed inventory data processing for task: {task_id}")
        return "Inventory data processed"
    except Exception as e:
        logger.error(f"Error processing inventory data: {str(e)}")
        raise


def update_database(data_type, **context):
    """Update database with processed data"""
    try:
        task_id = context["task_instance"].task_id
        logger.info(f"Starting database update for {data_type} in task: {task_id}")
        time.sleep(2)  # Simulate a heavy database operation
        logger.info(f"Completed database update for {data_type} in task: {task_id}")
        return f"Database updated for {data_type}"
    except Exception as e:
        logger.error(f"Error updating database: {str(e)}")
        raise


with DAG(
    "combined_example",
    default_args=default_args,
    description="A DAG demonstrating combined use of Task Groups and Pools",
    schedule_interval=None,
    catchup=False,
    tags=["example", "combined_patterns"],
) as dag:

    # Check if pool exists
    check_pool = PythonOperator(
        task_id="check_pool_exists",
        python_callable=check_pool_exists,
        provide_context=True,
    )

    # Create task groups for different data types
    with TaskGroup(group_id="data_processing") as data_processing:
        user_task = PythonOperator(
            task_id="process_user_data",
            python_callable=process_user_data,
            provide_context=True,
        )

        order_task = PythonOperator(
            task_id="process_order_data",
            python_callable=process_order_data,
            provide_context=True,
        )

        inventory_task = PythonOperator(
            task_id="process_inventory_data",
            python_callable=process_inventory_data,
            provide_context=True,
        )

        # Define relationships within the task group
        # These tasks can run in parallel
        [user_task, order_task, inventory_task]

    # Create task group for database updates
    with TaskGroup(group_id="database_updates") as database_updates:
        # These tasks will be limited by the database pool
        update_user_db = PythonOperator(
            task_id="update_user_database",
            python_callable=update_database,
            op_kwargs={"data_type": "user_data"},
            provide_context=True,
            pool=DB_POOL,
        )

        update_order_db = PythonOperator(
            task_id="update_order_database",
            python_callable=update_database,
            op_kwargs={"data_type": "order_data"},
            provide_context=True,
            pool=DB_POOL,
        )

        update_inventory_db = PythonOperator(
            task_id="update_inventory_database",
            python_callable=update_database,
            op_kwargs={"data_type": "inventory_data"},
            provide_context=True,
            pool=DB_POOL,
        )

    # Set dependencies
    check_pool >> data_processing >> database_updates
