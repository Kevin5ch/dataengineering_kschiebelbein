import requests

def get_fake_data(api : str = None):
  products = {}
  try:
    products : dict = requests.get(api)
  except Exception as e:
    print(e)
  finally:
    return products