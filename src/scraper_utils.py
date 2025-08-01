import requests

url = 'https://cdn.tsetmc.com/api/ClosingPrice/GetTradeTop/MostVisited/1/7'

response = requests.get(url)

print(response)
