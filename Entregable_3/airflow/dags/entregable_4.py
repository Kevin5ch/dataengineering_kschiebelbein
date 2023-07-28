from datetime import date, timedelta, datetime
import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow import DAG

def view_xcoms(ti):
  print("****Imprimo TI object****")
  print(ti)
  print("********")

with DAG(
  dag_id="entregable_4",
  start_date=datetime(2023, 7, 1), 
  catchup=False, 
  schedule=None, 
  tags=["CoderHouse", "Entregable 4", "Kevin Schiebelbein"],
) as dag:
    view_xcoms_data = PythonOperator(
        task_id="view_xcoms_fact", python_callable=view_xcoms
    ) # bash_requirements = BashOperator(
    view_xcoms_data
  #   task_id="install_requirements",
  #   bash_command="cd /opt/airflow/scripts/entregable2/requirements/ && pip3 install -r requirements.txt",
  # )
    
  # extract_products = BashOperator(
  #   task_id="extract_products",
  #   bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 extract_products.py",
  # )

  # transform_process = BashOperator(
  #   task_id="transform_products",
  #   bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 transform_products.py",
  # )

  # load_process = BashOperator(
  #   task_id="load_products",
  #   bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 load_products.py",
  # )
  
  # bash_requirements >> extract_products >> transform_process >> load_process