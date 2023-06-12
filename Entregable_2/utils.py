import requests

def getFakeData(api : str = None):
  products = {}
  try:
    products : dict = requests.get(api)
  except Exception as e:
    print(e)
  finally:
    return products