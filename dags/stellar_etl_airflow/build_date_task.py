import json

from datetime import timedelta
from subprocess import Popen

from airflow import DAG, AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


def build_date_task(dag):
    '''
    Creates a task to run the get_ledger_range_from_times command from the stellar-etl. The start time is the execution time,
    and the end time is the next execution time. Allows for retreiving the ledger range for the given execution. The range object
    sent as a string representation of a JSON object to the xcom, where it can be accessed by subsequent tasks.
    
    Parameter:
        dag - parent dag that the task will be attached to 
    Returns:
        the newly created task
    '''
    return BashOperator(
        task_id='get_ledger_range_from_times',
        bash_command='stellar-etl get_ledger_range_from_times -s {{ ts }} -e {{ next_execution_date.isoformat() }} --stdout',
        dag=dag,
        xcom_push=True,
    )