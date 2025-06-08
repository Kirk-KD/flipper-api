import requests

res = requests.get('http://localhost:8000/api/v1/top_flips')
print(res.json())
