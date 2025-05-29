import requests
import pandas as pd
from datetime import datetime, timezone

from config.config import (
    API_KEY, UNITS, LANG, BASE_URL
)

def get_weather_data(ciudad):
    params = {
        'q': ciudad,
        'appid': API_KEY,
        'units': UNITS,
        'lang': LANG
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Procesar y retornar solo los datos necesarios
        return {
            'ciudad': data.get('name'),
            'temperatura': data['main']['temp'],
            'humedad': data['main']['humidity'],
            'presion': data['main']['pressure'],
            'viento_velocidad': data['wind']['speed'],
            'descripcion_clima': data['weather'][0]['description'],
            'fecha_hora_consulta_utc': pd.to_datetime(data['dt'], unit='s', utc=True),
            'timestamp_ingestion': datetime.now(timezone.utc)
        }
    except Exception as e:
        print(f"Error al obtener datos para {ciudad}: {str(e)}")
        return None
