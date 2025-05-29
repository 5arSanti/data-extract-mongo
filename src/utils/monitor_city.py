from pymongo import MongoClient
import time

from config.config import (
    MONGO_CONNECTION_STRING
)

from utils.get_weather_data import get_weather_data

def monitor_city(ciudad, duracion_minutos=30, intervalo_segundos=120):
    try:
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print(f"Conexión exitosa a MongoDB para monitoreo de {ciudad}")
        
        db = client["clima"]
        coleccion = db["monitoreo_bogota_historico"]
        
        # Calcular número de iteraciones
        num_iteraciones = (duracion_minutos * 60) // intervalo_segundos
        
        print(f"\nIniciando monitoreo de {ciudad} por {duracion_minutos} minutos")
        print(f"Intervalo entre consultas: {intervalo_segundos} segundos")
        print(f"Número total de iteraciones: {num_iteraciones}")
        
        for i in range(num_iteraciones):
            print(f"\nIteración {i + 1}/{num_iteraciones}")
            
            # Obtener datos
            datos = get_weather_data(ciudad)
            if datos:
                # Insertar en MongoDB
                resultado = coleccion.insert_one(datos)
                print(f"Datos insertados exitosamente. ID: {resultado.inserted_id}")
                print(f"Temperatura: {datos['temperatura']}°C, Humedad: {datos['humedad']}%")
            
            # Esperar hasta la siguiente iteración
            if i < num_iteraciones - 1:  # No esperar después de la última iteración
                print(f"Esperando {intervalo_segundos} segundos hasta la siguiente consulta...")
                time.sleep(intervalo_segundos)
        
        print("\nMonitoreo completado exitosamente")
        
    except Exception as e:
        print(f"Error durante el monitoreo: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()
            print("Conexión a MongoDB cerrada")
