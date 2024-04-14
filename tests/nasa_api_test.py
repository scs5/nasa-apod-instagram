import requests

url = f"https://api.nasa.gov/planetary/apod?api_key=k4htQqwTLG5urLDjzHeHYlutVmkfFStJXe3bX7dC&hd=TRUE"
response = requests.get(url)
data = response.json()
print(data)