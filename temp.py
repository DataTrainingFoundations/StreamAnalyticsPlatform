import requests

API_KEY = "83C47EC1-A855-4637-B4B5-B1DA0471097B"

url = "https://www.airnowapi.org/aq/data/"

params = {
    "startDate": "2026-02-01T00",
    "endDate": "2026-02-03T23",
    "parameters": "PM25",
    "BBOX": "-85.13,33.30,-84.20,34.00",
    "dataType": "A",
    "format": "application/json",
    "verbose": 1,
    "API_KEY": API_KEY
}

response = requests.get(url, params=params)

data = response.json()

print(data[0])