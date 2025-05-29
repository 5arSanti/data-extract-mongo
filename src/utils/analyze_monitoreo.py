import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

from config.config import (
    MONGO_CONNECTION_STRING
)

def analyze_monitoreo():
    try:
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("Conexión exitosa a MongoDB para análisis de datos")
        
        db = client["clima"]
        coleccion = db["monitoreo_bogota_historico"]
        
        # Verificar total de documentos
        total_documentos = coleccion.count_documents({})
        print(f"\nTotal de documentos en la colección: {total_documentos}")
        
        # Mostrar rango de fechas en los datos
        primer_documento = coleccion.find_one({}, sort=[("fecha_hora_consulta_utc", 1)])
        ultimo_documento = coleccion.find_one({}, sort=[("fecha_hora_consulta_utc", -1)])
        
        if primer_documento and ultimo_documento:
            print("\nRango de fechas en los datos:")
            print(f"Primera medición: {primer_documento['fecha_hora_consulta_utc']}")
            print(f"Última medición: {ultimo_documento['fecha_hora_consulta_utc']}")
        
        # Pipeline de agregación
        pipeline = [
            # Fase 1: Agrupar por hora y calcular promedios
            {
                "$group": {
                    "_id": {
                        "hora": {"$hour": "$fecha_hora_consulta_utc"}
                    },
                    "temperatura_promedio": {"$avg": "$temperatura"},
                    "humedad_promedio": {"$avg": "$humedad"},
                    "viento_promedio": {"$avg": "$viento_velocidad"},
                    "conteo": {"$sum": 1},
                    "mediciones": {
                        "$push": {
                            "temperatura": "$temperatura",
                            "humedad": "$humedad",
                            "fecha_hora": "$fecha_hora_consulta_utc"
                        }
                    }
                }
            },
            # Fase 2: Ordenar por hora
            {
                "$sort": {"_id.hora": 1}
            }
        ]
        
        # Ejecutar pipeline
        resultados = list(coleccion.aggregate(pipeline))
        
        if not resultados:
            print("No se encontraron datos para analizar")
            return
        
        print(f"\nNúmero de grupos horarios encontrados: {len(resultados)}")
        
        # Convertir resultados a DataFrame
        df_analisis = pd.DataFrame([
            {
                'hora': r['_id']['hora'],
                'temperatura_promedio': round(r['temperatura_promedio'], 2),
                'humedad_promedio': round(r['humedad_promedio'], 2),
                'viento_promedio': round(r['viento_promedio'], 2),
                'conteo': r['conteo']
            }
            for r in resultados
        ])
        
        print("\nResultados del análisis por hora:")
        print(df_analisis.to_markdown(index=False))
        
        # Mostrar detalles de las mediciones por hora
        print("\nDetalles de las mediciones por hora:")
        for r in resultados:
            print(f"\nHora {r['_id']['hora']}:")
            for m in r['mediciones']:
                print(f"  Temperatura: {m['temperatura']}°C, Humedad: {m['humedad']}%, Fecha: {m['fecha_hora']}")
        
        # Crear gráfico de evolución de temperatura
        plt.figure(figsize=(12, 6))
        plt.plot(df_analisis['hora'], df_analisis['temperatura_promedio'], 
                marker='o', linestyle='-', linewidth=2, markersize=8)
        
        # Añadir etiquetas de valores
        for i, v in enumerate(df_analisis['temperatura_promedio']):
            plt.text(df_analisis['hora'][i], v + 0.2, f'{v:.1f}°C', 
                    ha='center', va='bottom')
        
        plt.title('Evolución de la Temperatura Promedio en Bogotá', fontsize=14, pad=20)
        plt.xlabel('Hora del Día (UTC)', fontsize=12)
        plt.ylabel('Temperatura Promedio (°C)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(df_analisis['hora'])
        plt.tight_layout()
        plt.show()
        print("\nGráfico de evolución de temperatura generado")
        
    except Exception as e:
        print(f"Error durante el análisis: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()
            print("Conexión a MongoDB cerrada")