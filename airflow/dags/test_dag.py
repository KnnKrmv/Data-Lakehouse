from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def first():
    return 5*5

def second(ti):
    result= ti.xcom_pull(task_ids='first_task')
    result2= result + 10  
    print(f"Result from first task: {result2}")



with DAG(dag_id="mydag_extract_data", 
         start_date=datetime(2024, 1, 1), 
         schedule_interval="@daily", 
         catchup=False) as dag:

    first_task = PythonOperator(
        task_id="first_task",
        python_callable=first
    )

    second_task = PythonOperator(
        task_id="second_task",  
        python_callable=second    
    )