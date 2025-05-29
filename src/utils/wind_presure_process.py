import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import json
from datetime import datetime

from config.config import (
    MONGO_CONNECTION_STRING
)

def wind_presure_process(df_clima):
    print("\n--- Análisis Comparativo de Viento y Presión ---")

    if not df_clima.empty:
        # Calcular promedios de viento y presión por ciudad
        resumen_viento_presion = df_clima.groupby('ciudad').agg(
            velocidad_viento_promedio=('viento_velocidad', 'mean'),
            presion_promedio=('presion', 'mean')
        ).round(2)
        
        print("\nResumen de velocidad del viento y presión por ciudad:")
        print(resumen_viento_presion.to_markdown())

        # Identificar ciudades con valores máximos
        ciudad_mayor_viento = df_clima.loc[df_clima['viento_velocidad'].idxmax()]
        ciudad_mayor_presion = df_clima.loc[df_clima['presion'].idxmax()]
        
        print(f"\nCiudad con mayor velocidad del viento: {ciudad_mayor_viento['ciudad']} ({ciudad_mayor_viento['viento_velocidad']:.2f} m/s)")
        print(f"Ciudad con mayor presión atmosférica: {ciudad_mayor_presion['ciudad']} ({ciudad_mayor_presion['presion']:.2f} hPa)")

        # Crear gráfico de barras doble
        plt.figure(figsize=(12, 6))
        
        # Crear dos subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Gráfico de velocidad del viento
        sns.barplot(x='ciudad', y='viento_velocidad', data=df_clima, ax=ax1, palette='viridis')
        ax1.set_title('Velocidad Promedio del Viento por Ciudad', fontsize=14, pad=20)
        ax1.set_xlabel('Ciudad', fontsize=12)
        ax1.set_ylabel('Velocidad del Viento (m/s)', fontsize=12)
        ax1.tick_params(axis='x', rotation=45)
        
        # Gráfico de presión atmosférica
        sns.barplot(x='ciudad', y='presion', data=df_clima, ax=ax2, palette='plasma')
        ax2.set_title('Presión Atmosférica Promedio por Ciudad', fontsize=14, pad=20)
        ax2.set_xlabel('Ciudad', fontsize=12)
        ax2.set_ylabel('Presión (hPa)', fontsize=12)
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.show()
        print("Gráficos de Velocidad del Viento y Presión Atmosférica generados.")

    print("\n--- Almacenamiento de Datos Históricos en MongoDB ---")

    if not df_clima.empty:
        try:
            client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            print("Conexión exitosa a MongoDB.")

            db = client["clima"]
            coleccion_historica = db["datos_meteorologicos_historico_viento"]

            # Preparar datos para inserción con timestamp
            datos_para_mongo = df_clima.to_dict("records")
            for doc in datos_para_mongo:
                doc['timestamp_insercion'] = datetime.now()

            print(f"\nInsertando {len(datos_para_mongo)} documentos en la colección histórica...")
            insert_result = coleccion_historica.insert_many(datos_para_mongo)
            print(f"Número de documentos insertados: {len(insert_result.inserted_ids)}")

            # Verificar datos insertados
            print("\nVerificando datos históricos insertados:")
            for doc in coleccion_historica.find({}, {'ciudad': 1, 'viento_velocidad': 1, 'presion': 1, 'timestamp_insercion': 1, '_id': 0}).limit(3):
                print(json.dumps(doc, indent=2, default=str))

            client.close()
            print("\nConexión a MongoDB cerrada exitosamente.")

        except Exception as e:
            print(f"ERROR: Ocurrió un error al almacenar datos históricos en MongoDB: {e}")
