from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime
import boto3 

def bucket_folder_creators():
    s3 = boto3.client('s3',
         endpoint_url='http://minio:9000/',
         aws_access_key_id ='minioadmin',
         aws_secret_access_key='minioadmin123'
         ) 
    bucketname = 'warehouse' 
    folders = ['test/test1/']

    for folder in folders:
        s3.put_object(Bucket = bucketname, Key = folder)

with DAG(
    dag_id='folder_creator',
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False    
    ) as dag:
        create_folder=PythonOperator(
                task_id='create_minio_folders',
                python_callable = bucket_folder_creators
        )
