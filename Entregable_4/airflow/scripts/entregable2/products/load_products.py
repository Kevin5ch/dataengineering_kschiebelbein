import redshift_connector
from dotenv import dotenv_values
from sqlalchemy import create_engine
import pandas as pd
# Variable que marca si guardo los datos en DW (solo funciona para quien tiene las cuentas de AWS RedShift)
store_in_dw = False

df = pd.read_parquet('/tmp/products_etl.parquet')

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
  