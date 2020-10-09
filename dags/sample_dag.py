
from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import random

args = {
    'owner' : 'ayush',
    'start_date' : days_ago(1)
}


dag = DAG(dag_id='wassap_dag', default_args=args, schedule_interval=None)


def run_this_func1(**context):
    passed_value = 1
    context['ti'].xcom_push(key="pass_val", value=passed_value)
    print ('passed_value', passed_value)

def run_this_func2(**context):
    received_value = context['ti'].xcom_pull(key="pass_val")
    passed_value = received_value + 1
    context['ti'].xcom_push(key="pass_val", value=passed_value)
    print ('received_value', received_value)
    print ('passed_value', passed_value)


def run_this_func3(**context):
    received_value = context['ti'].xcom_pull(key="pass_val")
    passed_value = received_value * 10
    context['ti'].xcom_push(key="pass_val", value=passed_value)
    print ('received_value', received_value)
    print ('passed_value', passed_value)


def run_this_func4(**context):
    received_value = context['ti'].xcom_pull(key="pass_val")
    print ('received_value', received_value)

def branch_func(**context):
    if random.random() < 0.5:
        return 'run_this_task2'
    else:
        return 'run_this_task3'


with dag:
    run_this_task1 = PythonOperator(
        task_id = "run_this_task1",
        python_callable = run_this_func1,
        provide_context = True,
        retries = 10,
        retry_delay = timedelta(seconds=5)
    )

    run_this_task2 = PythonOperator(
        task_id = "run_this_task2",
        python_callable = run_this_func2,
        provide_context = True,
        retries = 10,
        retry_delay = timedelta(seconds=5)
    )    

    run_this_task3 = PythonOperator(
        task_id = "run_this_task3",
        python_callable = run_this_func3,
        provide_context = True,
        retries = 10,
        retry_delay = timedelta(seconds=5)
    )

    run_this_task_branch = BranchPythonOperator(
        task_id = "run_this_task_branch",
        python_callable = branch_func,
        provide_context = True
    ) 

    run_this_task4 = PythonOperator(
        task_id = "run_this_task4",
        python_callable = run_this_func4,
        provide_context = True,
        retries = 10,
        retry_delay = timedelta(seconds=5)
    )

    

    #DAG
    run_this_task1 >> run_this_task_branch >> [run_this_task2, run_this_task3] >> run_this_task4 