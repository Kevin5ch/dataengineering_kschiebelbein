from datetime import date, timedelta, datetime
import json
import pandas as pd
from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(
  start_date=datetime(2023, 7, 1), 
  catchup=False, 
  schedule=None, 
  tags=["CoderHouse", "Entregable 3", "Kevin Schiebelbein"],
)
def entregable_3():

  bash_requirements = BashOperator(
    task_id="install_requirements",
    bash_command="cd /opt/airflow/scripts/entregable2/requirements/ && pip3 install -r requirements.txt",
  )
    
  fetch_products = BashOperator(
    task_id="fetch_products_data",
    bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 extract_products.py",
  )

  etl_process = BashOperator(
    task_id="etl_products",
    bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 transform_products.py",
  )

  load_process = BashOperator(
    task_id="load_products",
    bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 load_products.py",
  )
  
  bash_requirements >> fetch_products >> etl_process >> load_process

entregable_3()