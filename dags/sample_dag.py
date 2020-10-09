
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

args = {
    'owner' : 'ayush',
    'start_date' : days_ago(1)
}


dag = DAG(dag_id='wassap_dag', default_args=args, schedule_interval=None)


def run_this_func1(**context):
    print ("hello_dag")

def run_this_func2(**contect):
    # print ("hello_dag")
    print (str(5/0))

def run_this_func3(**context):
    print ("hello_dag3")



with dag:
    run_this_task1 = PythonOperator(
        task_id = "run_this_task1",
        python_callable = run_this_func1,
        provide_context = True
    )

    run_this_task2 = PythonOperator(
        task_id = "run_this_task2",
        python_callable = run_this_func2,
        provide_context = True
    )    

    run_this_task3 = PythonOperator(
        task_id = "run_this_task3",
        python_callable = run_this_func3,
        provide_context = True
    )

    #DAG
    run_this_task1 >> run_this_task2 >> run_this_task3 