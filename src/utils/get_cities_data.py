import requests
import pandas as pd
import time

from config.config import (
    CITIES, API_KEY, UNITS, LANG, BASE_URL, all_weather_data
)

def get_cities_data():
    for city in CITIES: 
        params = {
            'q': city,
            'appid': API_KEY,
            'units': UNITS,
            'lang': LANG
        }

        max_retries = 3
        retry_delay = 5 # segundos
        for attempt in range(max_retries):
            try:
                print(f"Consultando datos para {city} (Intento {attempt + 1}/{max_retries})...")
                response = requests.get(BASE_URL, params=params, timeout=15)
                response.raise_for_status()

                data = response.json()

                weather_info = {
                    'ciudad': data.get('name'),
                    'temperatura': data['main']['temp'],
                    'sensacion_termica': data['main']['feels_like'],
                    'temp_min': data['main']['temp_min'],
                    'temp_max': data['main']['temp_max'],
                    'humedad': data['main']['humidity'],
                    'presion': data['main']['pressure'],
                    'descripcion_clima': data['weather'][0]['description'],
                    'viento_velocidad': data['wind']['speed'],
                    'nubes_porcentaje': data['clouds']['all'],
                    'fecha_hora_consulta_utc': pd.to_datetime(data['dt'], unit='s', utc=True)
                }
                all_weather_data.append(weather_info)
                print(f"Datos de {city} extraídos exitosamente.")
                break

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == 401:
                    print(f"Error 401 (Unauthorized) para {city}: API Key inválida o no activa. Verifique su clave y espere su activación completa. No se reintentará.")
                    break
                elif status_code == 404:
                    print(f"Error 404 (Not Found) para {city}: Ciudad no encontrada o error en el nombre. Verifique el nombre. No se reintentará.")
                    break
                elif status_code == 429:
                    print(f"Error 429 (Too Many Requests) para {city}: Límite de solicitudes excedido. Esperando {retry_delay} segundos antes de reintentar...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif 400 <= status_code < 500:
                    print(f"Error del cliente (HTTP {status_code}) al obtener datos de {city}. Respuesta: {e.response.text}. No se reintentará para este tipo de error.")
                    break
                elif 500 <= status_code < 600:
                    print(f"Error del servidor (HTTP {status_code}) al obtener datos de {city}. Esperando {retry_delay} segundos antes de reintentar...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print(f"Error HTTP inesperado ({status_code}) para {city}. No se reintentará. Detalles: {e.response.text}")
                    break

            except requests.exceptions.ConnectionError as e:
                print(f"Error de conexión para {city}: No se pudo establecer conexión con el servidor. Verifique su conexión a internet. Esperando {retry_delay} segundos antes de reintentar...")
                time.sleep(retry_delay)
                retry_delay *= 2
            except requests.exceptions.Timeout as e:
                print(f"Error de tiempo de espera (Timeout) para {city}: La solicitud a la API tardó demasiado. Esperando {retry_delay} segundos antes de reintentar...")
                time.sleep(retry_delay)
                retry_delay *= 2
            except requests.exceptions.RequestException as e:
                print(f"Error general en la solicitud para {city} (no HTTP o conexión): {e}. No se reintentará.")
                break
            except KeyError as e:
                print(f"Error al procesar datos para {city}: Falta una clave esperada ({e}) en la respuesta JSON. La estructura de la API puede haber cambiado o ser incompleta para esta ciudad. No se reintentará.")
                break
            except ValueError as e:
                print(f"Error de valor al procesar datos JSON para {city}: {e}. La respuesta no es un JSON válido o tiene un formato inesperado. No se reintentará.")
                break
            except Exception as e:
                print(f"Ocurrió un error inesperado para {city}: {e}. No se reintentará.")
                break
        else:
            print(f"Fallaron todos los reintentos para {city}. No se pudieron obtener los datos.")
