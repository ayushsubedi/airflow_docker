
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

args = {
    'owner' : 'ayush',
    'start_date' : days_ago(1)
}

dag = DAG(dag_id='sample_dag', default_args=args, schedule_interval=None)

def run_this_func(**context):
    print ("hello_dag")

def run_this_func2(**context):
    print ("hello_dag2")

with dag:
    run_this_task = PythonOperator(
        task_id = "run_this",
        python_callable = run_this_func,
        provide_context = True
    )

    run_this_task2 = PythonOperator(
        task_id = "run_this2",
        python_callable = run_this_func2,
        provide_context = True
    )

    #DAG
    run_this_task >> run_this_task2