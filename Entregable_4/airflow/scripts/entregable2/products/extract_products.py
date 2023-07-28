import logging
from utils import get_fake_data
import json

products = []
try:
  logging.info("Busco productos en la web")
  api = "https://fakestoreapi.com/products"
  result = get_fake_data(api)
  products = json.loads(result.text) # Devuelve una lista de diccionarios
except Exception as e:
  print(e)
finally:
  with open("/tmp/products.json", "w") as outfile:
    json.dump(products, outfile)