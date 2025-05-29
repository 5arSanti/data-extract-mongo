"""
Configuration settings for the weather data analysis project.
"""

API_KEY = '3be2c91d3c61bcc3f81b8c701fe1fbf5'
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'

CITIES = ['Bogota', 'Medellin', 'Cali', 'London', 'New York', 'Sydney', 'Cairo', 'Rio de Janeiro']
UNITS = 'metric'
LANG = 'es'
UNITS_SYMBOL = 'C' if UNITS == 'metric' else 'F'

MONGO_CONNECTION_STRING = "mongodb+srv://johelsariasb:u3znwTHJEAJhgJmt@data-extract-mongo.a3o9uaa.mongodb.net/?retryWrites=true&w=majority&appName=data-extract-mongo"
MONITORING_INTERVAL = 300
MONITORING_DURATION = 60

all_weather_data = []
