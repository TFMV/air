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
API_POOL = "api_rate_limit_pool"
MAX_CONCURRENT_REQUESTS = 2  # Maximum number of concurrent API calls allowed


@provide_session
def check_pool_exists(session, **context):
    """Check if the required pool exists"""
    pool = session.query(Pool).filter(Pool.pool == API_POOL).first()
    if not pool:
        raise Exception(
            f"Pool '{API_POOL}' does not exist. Please create it in the Airflow UI or via CLI:\n"
            f"airflow pools set {API_POOL} {MAX_CONCURRENT_REQUESTS} 'Limits concurrent API calls'"
        )
    logger.info(f"Pool '{API_POOL}' exists with {pool.slots} slots")
    return f"Pool '{API_POOL}' verified"


def make_api_call(api_endpoint, **context):
    """Simulate making an API call to a specific endpoint"""
    try:
        task_id = context["task_instance"].task_id
        logger.info(f"Starting API call to {api_endpoint} for task: {task_id}")
        time.sleep(random.uniform(1, 2))  # Simulate API call duration
        logger.info(f"Completed API call to {api_endpoint} for task: {task_id}")
        return f"API call to {api_endpoint} completed"
    except Exception as e:
        logger.error(f"Error in API call to {api_endpoint}: {str(e)}")
        raise


with DAG(
    "pool_example",
    default_args=default_args,
    description="A DAG demonstrating Pools for API rate limiting with parallel processing",
    schedule_interval=None,
    catchup=False,
    tags=["example", "pool"],
) as dag:

    # Check if pool exists
    check_pool = PythonOperator(
        task_id="check_pool_exists",
        python_callable=check_pool_exists,
        provide_context=True,
    )

    # Create task groups for different API endpoints
    with TaskGroup(group_id="user_api_calls") as user_api_calls:
        # These tasks will run in parallel but be limited by the pool
        get_user_profile = PythonOperator(
            task_id="get_user_profile",
            python_callable=make_api_call,
            op_kwargs={"api_endpoint": "user/profile"},
            provide_context=True,
            pool=API_POOL,
        )

        get_user_preferences = PythonOperator(
            task_id="get_user_preferences",
            python_callable=make_api_call,
            op_kwargs={"api_endpoint": "user/preferences"},
            provide_context=True,
            pool=API_POOL,
        )

    with TaskGroup(group_id="order_api_calls") as order_api_calls:
        # These tasks will run in parallel but be limited by the pool
        get_order_status = PythonOperator(
            task_id="get_order_status",
            python_callable=make_api_call,
            op_kwargs={"api_endpoint": "order/status"},
            provide_context=True,
            pool=API_POOL,
        )

        get_order_details = PythonOperator(
            task_id="get_order_details",
            python_callable=make_api_call,
            op_kwargs={"api_endpoint": "order/details"},
            provide_context=True,
            pool=API_POOL,
        )

    # Set dependencies
    # First check the pool exists
    # Then run user API calls in parallel
    # Then run order API calls in parallel
    check_pool >> user_api_calls >> order_api_calls
