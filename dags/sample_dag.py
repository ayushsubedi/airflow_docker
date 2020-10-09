
from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator
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
    # print ("hello_dag")
    # print (str(5/0))
    received_value = context['ti'].xcom_pull(key="pass_val")
    passed_value = received_value + 1
    context['ti'].xcom_push(key="pass_val", value=passed_value)
    if (random.random() > .7):
        print (str(5/0))
    else:
        print ('received_value', received_value)
        print ('passed_value', passed_value)


def run_this_func3(**context):
    received_value = context['ti'].xcom_pull(key="pass_val")
    print ('received_value', received_value)



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

    #DAG
    run_this_task1 >> run_this_task2 >> run_this_task3 