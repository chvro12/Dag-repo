from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.task import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
import requests
import boto3
import json

with DAG(
    dag_id="weather",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )
    
    @task
    def fetch_openmeteo(**kwargs):
        lon, lat = 4.85, 45.7
        
        results = requests.get(
            f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,wind_speed_10m"
        )
        
        if results.status_code != 200:
            raise Exception(f"API returned status code {results.status_code}")
        
        print(results.json())
        
        execution_time = kwargs.get('ts_nodash')
        s3_key = f"SThiam/weather/bronze/openmeteo/{execution_time}/openmeteo_{execution_time}.json"
        
        hook = S3Hook(aws_conn_id="aws_default")
        s3_client = hook.get_conn()
        
        s3_client.put_object(
            Bucket="esgi-lyon-iabd-m2-cloud",
            Key=s3_key,
            Body=results.text.encode('utf-8')
        )
        
        s3_path = f"s3://esgi-lyon-iabd-m2-cloud/{s3_key}"
        print(f"Data uploaded to: {s3_path}")
        
        return s3_path
    
    @task
    def fetch_7timer(**kwargs):
        lon, lat = 4.85, 45.7
        
        results = requests.get(
            f"http://www.7timer.info/bin/api.pl?lon={lon}&lat={lat}&product=civil&output=json&unit=Metric"
        )
        
        if results.status_code != 200:
            raise Exception(f"API returned status code {results.status_code}")
        
        print(results.json())
        
        execution_time = kwargs.get('ts_nodash')
        s3_key = f"SThiam/weather/bronze/7timer/{execution_time}/7timer_{execution_time}.json"
        
        hook = S3Hook(aws_conn_id="aws_default")
        s3_client = hook.get_conn()
        
        s3_client.put_object(
            Bucket="esgi-lyon-iabd-m2-cloud",
            Key=s3_key,
            Body=results.text.encode('utf-8')
        )
        
        s3_path = f"s3://esgi-lyon-iabd-m2-cloud/{s3_key}"
        print(f"Data uploaded to: {s3_path}")
        
        return s3_path
    
    fetch_openmeteo_task = fetch_openmeteo()
    fetch_7timer_task = fetch_7timer()
    
    merge_weather_data = LambdaInvokeFunctionOperator(
        task_id="merge_weather_data",
        function_name="EsgiIabdM2SThiamAirflowWeather",
        payload='{"openmeteo_key": "{{ ti.xcom_pull(task_ids=\'fetch_openmeteo\') }}", "7timer_key": "{{ ti.xcom_pull(task_ids=\'fetch_7timer\') }}"}',
        aws_conn_id="aws_default"
    )
    
    start >> [fetch_openmeteo_task, fetch_7timer_task] >> merge_weather_data

