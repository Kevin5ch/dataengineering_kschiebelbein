from datetime import date, timedelta, datetime
import logging
import json
import requests
import pandas as pd
# from airflow.scripts.entregable2.products.utils import getFakeData
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine
import redshift_connector
from dotenv import dotenv_values
#from airflow.operators.empty import EmptyOperator


@dag(
  start_date=datetime(2023, 7, 1), 
  catchup=False, 
  schedule=None, 
  tags=["CoderHouse", "Entregable 3", "Kevin Schiebelbein"],
)
def entregable_3():

  def getFakeData(api : str = None):
    products = {}
    try:
      products : dict = requests.get(api)
    except Exception as e:
      print(e)
    finally:
      return products

  bash_requirements = BashOperator(
    task_id="install_entregable2_requirements",
    bash_command="cd /opt/airflow/dags && pip3 install -r ../scripts/entregable2/requirements/requirements.txt",
  )
    
  @task(task_id="fetch_products_data")
  def get_products(ds=None, **context):
    products = []
    try:
      logging.info("Busco productos en la web")
      api = "https://fakestoreapi.com/products"
      result = getFakeData(api)
      products = json.loads(result.text) # Devuelve una lista de diccionarios
    except Exception as e:
      print(e)
    finally:
      with open("/tmp/products.json", "w") as outfile:
        json.dump(products, outfile)
  
  fetch_products = get_products()
  
  @task(task_id="etl_products_and_store_data")
  def products_etl_process():
    store_in_dw = False
    with(open("/tmp/products.json", "r")) as products_json:
      products = json.loads(products_json.read())
      df = pd.DataFrame(products)
      df = pd.concat([df.drop(['rating'], axis=1), df['rating'].apply(pd.Series)], axis=1)
      df['status'] = ['On Sale' if x < 50 else '' for x in df['price']]
      df["fecha"] = date.today()
      manana = date.today() + timedelta(days=1) 
      df.loc[len(df.index)] = [20, "Producto nuevo 1 - Lente camara", 500, "Alguna description 1", "IT", "https://johnstillk8.scusd.edu/sites/main/files/main-images/camera_lense_0.jpeg", 3.5, 9, "On Sale", manana]
      df.loc[len(df.index)] = [21, "Producto nuevo 2 - Notebook", 3000, "Alguna description 2", "IT", "https://www.oberlo.com/media/1603969791-image-1.jpg?fit=max&fm=webp&w=1824", 6, 25, "On Sale", manana]
      pasado = date.today() + timedelta(days=2) 
      df.loc[len(df.index)] = [21, "Producto nuevo editado 1 - Notebook", 3500, "Alguna description 2", "IT", "https://www.oberlo.com/media/1603969791-image-1.jpg?fit=max&fm=webp&w=1824", 6, 25, "On Sale", pasado]
      #categories = df.groupby(["category"]).count()
      #top_10_products = df.sort_values("rate", ascending=False).head(10)
      df = df.drop(['description'], axis=1)
      
      # Variable que marca si guardo los datos en DW (solo funciona para quien tiene las cuentas de AWS RedShift)
      if store_in_dw:
        # Connects to Redshift cluster using AWS credentials
        config = dotenv_values(".env")
        driver = config["DRIVER"]
        host = config["HOST"]
        db = config["DB"]
        user = config["USER"]
        password = config["PASSDW"]
        port = config["PORT"] 
    
        connection = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        with redshift_connector.connect(host=host,database=db,user=user,password=password) as conn:
          with conn.cursor() as cursor:
            conn.autocommit = True
            cursor: redshift_connector.Cursor = conn.cursor()

            # Creo tabla stage para dejar los nuevos registros hasta que haga el insert incremental en la tabla final
            tabla_staging = f"""
              CREATE TABLE IF NOT EXISTS products_staging (
              id INTEGER,
              title VARCHAR(128),
              price FLOAT8,
              category VARCHAR(256),
              image TEXT,
              rate FLOAT8,
              count FLOAT8,
              status VARCHAR(32),
              fecha DATE
              ) DISTKEY(id) SORTKEY(rate);
            """
            truncate_table = "TRUNCATE TABLE products_staging;"
            try:
              cursor.execute(tabla_staging)
              cursor.execute(truncate_table)
              df.to_sql('products_staging', connection, index=False, if_exists='replace')
              try:
                tabla = f"""
                  CREATE TABLE IF NOT EXISTS products (
                  id INTEGER,
                  title VARCHAR(128),
                  price FLOAT8,
                  category VARCHAR(256),
                  image TEXT,
                  rate FLOAT8,
                  count FLOAT8,
                  status VARCHAR(32),
                  fecha DATE
                  ) DISTKEY(id) SORTKEY(rate);
                """
                cursor.execute(tabla)

                # Comparo identifico los registros nuevos en la tabla staging y los guardo en la tabla final
                cursor.execute("begin transaction;")
                cursor.execute(f"delete from products using products_staging where products.id = products_staging.id and products.fecha >= '{date.today()}'")
                cursor.execute(f"insert into products select * from products_staging")
                cursor.execute("end transaction;")
                cursor.close()
              except Exception as e:
                cursor.close()
                print(f"Error al guardar los datos: {e}") 
            except Exception as e:
              print(f"Error al guardar los datos: {e}")
            return None
        
  etl_process = products_etl_process()
  
  bash_requirements >> fetch_products >> etl_process 

entregable_3()