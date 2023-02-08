import datetime as dt
import requests,json

BASE_URL = "https://openweathermap.org/data/2.5/weather?q="
API_KEY ="ee636f46ae091c02e5ba5b336d9a1461"
CITY="London"

def kelvin_to_celsius_fahrenheit(kelvin):
    celsius= kelvin - 273.15
    fahrenheit = celsius * (9/5) +32
    return celsius,fahrenheit

url = BASE_URL + CITY + "appid=" + API_KEY 

response =requests.get(url).json()
print(response)

