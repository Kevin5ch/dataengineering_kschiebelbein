from utils import getFakeData

api = "https://fakestoreapi.com/products"
result = getFakeData(api)
products = result.text
print(products)
