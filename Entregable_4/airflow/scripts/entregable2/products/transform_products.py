from datetime import date, timedelta
import json
import pandas as pd

def upload_on_sale_products(**context):
  with(open("/tmp/products.json", "r")) as products_json:
    count_on_sale = 0
    context['ti'].xcom_push(key='q_on_sale_products', value=count_on_sale)
    products = json.loads(products_json.read())
    df = pd.DataFrame(products)
    df = pd.concat([df.drop(['rating'], axis=1), df['rating'].apply(pd.Series)], axis=1)
    df['status'] = ['On Sale' if x < 50 else '' for x in df['price']]
    count_on_sale = len(df[(df["status"] == "On Sale")])
    #context['ti'].xcom_push(key='q_on_sale_products', value=count_on_sale)
    df["fecha"] = date.today()
    manana = date.today() + timedelta(days=1) 
    df.loc[len(df.index)] = [20, "Producto nuevo 1 - Lente camara", 500, "Alguna description 1", "IT", "https://johnstillk8.scusd.edu/sites/main/files/main-images/camera_lense_0.jpeg", 3.5, 9, "On Sale", manana]
    df.loc[len(df.index)] = [21, "Producto nuevo 2 - Notebook", 3000, "Alguna description 2", "IT", "https://www.oberlo.com/media/1603969791-image-1.jpg?fit=max&fm=webp&w=1824", 6, 25, "On Sale", manana]
    pasado = date.today() + timedelta(days=2) 
    df.loc[len(df.index)] = [21, "Producto nuevo editado 1 - Notebook", 3500, "Alguna description 2", "IT", "https://www.oberlo.com/media/1603969791-image-1.jpg?fit=max&fm=webp&w=1824", 6, 25, "On Sale", pasado]
    #categories = df.groupby(["category"]).count()
    #top_10_products = df.sort_values("rate", ascending=False).head(10)
    df = df.drop(['description'], axis=1)
    df.to_parquet("/tmp/products_etl.parquet")