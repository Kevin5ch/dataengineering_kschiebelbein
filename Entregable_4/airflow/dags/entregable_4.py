from datetime import date, timedelta, datetime
import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def enviar_correo(destinatario, asunto, mensaje):
    try:
        # Configuración del servidor SMTP
        servidor_smtp = 'smtp.gmail.com'
        puerto_smtp = 587
        usuario_smtp = Variable.get('SMTP_EMAIL_FROM')
        contraseña_smtp = Variable.get('SMTP_APPLICATION_PASSWORD')

        # Crear el mensaje de correo electrónico
        msg = MIMEMultipart()
        msg['From'] = usuario_smtp
        msg['To'] = destinatario
        msg['Subject'] = asunto

        # Agregar el cuerpo del mensaje
        msg.attach(MIMEText(mensaje, 'plain'))

        # Iniciar la conexión con el servidor SMTP
        servidor = smtplib.SMTP(servidor_smtp, puerto_smtp)
        servidor.starttls()

        # Autenticación en el servidor SMTP
        servidor.login(usuario_smtp, contraseña_smtp)

        # Enviar el correo electrónico
        servidor.sendmail(usuario_smtp, destinatario, msg.as_string())

        # Cerrar la conexión con el servidor SMTP
        servidor.quit()
        print("Envio de correo exitoso")
    except Exception as e:
      print(e)


def enviar_correo_ofertas(**context):
    # Uso del método para enviar un correo
    destinatario = Variable.get('SMTP_EMAIL_TO')
    asunto = '¡Nuevas ofertas!'
    cantidad_premios = context['ti'].xcom_pull(key = "q_on_sale_products", task_ids = "transform_process")
    mensaje = f"Hay {cantidad_premios} productos en oferta esperando a que te los lleves"
    enviar_correo(destinatario, asunto, mensaje)

def on_sale_sensor(**context):
  # Armo un sensor custom para capturar cuando haya nuevos productos en oferta
  hay_oferta = False
  hay_oferta = context['ti'].xcom_pull(key = "q_on_sale_products", task_ids = "transform_process")
  if hay_oferta:
    return True
  else:
    print("No hay ofertas!!!!!")
    return False

def upload_on_sale_products(**context):
  # Cargo los productos en un DataFrame y los trato para guardarlos en el Warehouse
  with(open("/tmp/products.json", "r")) as products_json:
    count_on_sale = 0
    products = json.loads(products_json.read())
    df = pd.DataFrame(products)
    df = pd.concat([df.drop(['rating'], axis = 1), df['rating'].apply(pd.Series)], axis=1)
    df['status'] = ['On Sale' if x < 50 else '' for x in df['price']]
    count_on_sale = len(df[(df["status"] == "On Sale")])
    context['ti'].xcom_push(key = 'q_on_sale_products', value = count_on_sale)
    df["fecha"] = date.today()
    manana = date.today() + timedelta(days = 1) 
    df.loc[len(df.index)] = [20, "Producto nuevo 1 - Lente camara", 500, "Alguna description 1", "IT", "https://johnstillk8.scusd.edu/sites/main/files/main-images/camera_lense_0.jpeg", 3.5, 9, "On Sale", manana]
    df.loc[len(df.index)] = [21, "Producto nuevo 2 - Notebook", 3000, "Alguna description 2", "IT", "https://www.oberlo.com/media/1603969791-image-1.jpg?fit=max&fm=webp&w=1824", 6, 25, "On Sale", manana]
    pasado = date.today() + timedelta(days = 2) 
    df.loc[len(df.index)] = [21, "Producto nuevo editado 1 - Notebook", 3500, "Alguna description 2", "IT", "https://www.oberlo.com/media/1603969791-image-1.jpg?fit=max&fm=webp&w=1824", 6, 25, "On Sale", pasado]
    df = df.drop(['description'], axis = 1)
    df.to_parquet("/tmp/products_etl.parquet")

with DAG(
  dag_id="entregable_4",
  start_date=datetime(2023, 7, 1), 
  catchup=False, 
  schedule=None, 
  tags=["CoderHouse", "Entregable 4", "Kevin Schiebelbein"],
) as dag:
  bash_requirements = BashOperator(
    task_id="install_requirements",
    bash_command="cd /opt/airflow/scripts/entregable2/requirements/ && pip3 install -r requirements.txt",
  )
    
  extract_products = BashOperator(
    task_id="extract_products",
    bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 extract_products.py",
  )

  transform_process = PythonOperator(
      task_id="transform_process", python_callable=upload_on_sale_products
  ) 

  send_email_task = PythonOperator(
      task_id="send_email_process", python_callable=enviar_correo_ofertas
  ) 
  
  check_sale_availability = PythonSensor(
    task_id="check_sale_availability",
    poke_interval=10,
    timeout=3600,
    mode="poke",
    python_callable=on_sale_sensor
  )

  load_process = BashOperator(
    task_id="load_products",
    bash_command="cd /opt/airflow/scripts/entregable2/products/ && python3 load_products.py",
  )
  
  check_sale_availability >> send_email_task
  bash_requirements >> extract_products >> transform_process >> load_process
