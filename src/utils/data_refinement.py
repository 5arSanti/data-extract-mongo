import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import json
from datetime import datetime

from config.config import (
    MONGO_CONNECTION_STRING
)

def data_refinement():
    print("\n--- Ejercicio 2: Refinamiento de Datos y Consulta Avanzada ---")

    try:
        # Conectar a MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("Conexión exitosa a MongoDB para análisis avanzado.")

        # Obtener la colección histórica
        db = client["clima"]
        coleccion_historica = db["datos_meteorologicos_historico_viento"]

        # Verificar si hay datos en la colección
        conteo_documentos = coleccion_historica.count_documents({})
        print(f"\nNúmero total de documentos en la colección: {conteo_documentos}")

        if conteo_documentos == 0:
            print("ADVERTENCIA: La colección está vacía. Asegúrese de que los datos se hayan insertado correctamente en el Ejercicio 1.")
            print("Verificando la colección original...")
            coleccion_original = db["datos_meteorologicos"]
            conteo_original = coleccion_original.count_documents({})
            print(f"Número de documentos en la colección original: {conteo_original}")
            
            if conteo_original > 0:
                print("\nCopiando datos de la colección original a la histórica...")
                documentos_originales = list(coleccion_original.find({}))
                for doc in documentos_originales:
                    doc['timestamp_insercion'] = datetime.now()
                coleccion_historica.insert_many(documentos_originales)
                print("Datos copiados exitosamente.")
            else:
                print("ERROR: Ambas colecciones están vacías. No hay datos para analizar.")
                raise Exception("No hay datos disponibles para el análisis")

        # Carga selectiva de datos con proyección
        print("\nCargando datos selectivos desde MongoDB...")
        cursor = coleccion_historica.find(
            {},
            {
                'ciudad': 1,
                'temperatura': 1,
                'humedad': 1,
                'fecha_hora_consulta_utc': 1,
                '_id': 0
            }
        ).sort('fecha_hora_consulta_utc', 1)

        # Convertir a DataFrame
        df_analisis = pd.DataFrame(list(cursor))
        
        if df_analisis.empty:
            print("ERROR: No se pudieron cargar datos en el DataFrame.")
            raise Exception("DataFrame vacío después de la carga de datos")
        
        print(f"\nDatos cargados exitosamente. Número de registros: {len(df_analisis)}")
        
        # Convertir fecha_hora_consulta_utc a datetime si no lo es
        df_analisis['fecha_hora_consulta_utc'] = pd.to_datetime(df_analisis['fecha_hora_consulta_utc'])
        
        # Convertir UTC a hora local (Colombia está en UTC-5)
        df_analisis['fecha_hora_local'] = df_analisis['fecha_hora_consulta_utc'] - pd.Timedelta(hours=5)
        
        # Extraer hora del día (tanto UTC como local)
        df_analisis['hora_del_dia_utc'] = df_analisis['fecha_hora_consulta_utc'].dt.hour
        df_analisis['hora_del_dia_local'] = df_analisis['fecha_hora_local'].dt.hour
        
        print("\nDistribución de horas (UTC y Local):")
        print("\nHoras UTC:")
        print(df_analisis['hora_del_dia_utc'].value_counts().sort_index().to_markdown())
        print("\nHoras Locales (UTC-5):")
        print(df_analisis['hora_del_dia_local'].value_counts().sort_index().to_markdown())
        
        # Filtrar datos usando hora local
        df_filtrado = df_analisis[
            (df_analisis['hora_del_dia_local'] >= 8) & 
            (df_analisis['hora_del_dia_local'] <= 18)
        ]
        
        if df_filtrado.empty:
            print("\nADVERTENCIA: No hay datos en el rango horario local especificado (8:00 - 18:00)")
            print("Analizando todos los datos disponibles...")
            df_filtrado = df_analisis  # Usar todos los datos si no hay en el rango especificado
        
        print("\nVista previa del DataFrame filtrado:")
        print(df_filtrado.head().to_markdown(index=False))
        
        # Análisis de desviación térmica
        print("\n--- Análisis de Temperaturas por Ciudad ---")
        
        # Calcular estadísticas por ciudad
        estadisticas_por_ciudad = df_filtrado.groupby('ciudad').agg({
            'temperatura': ['mean', 'min', 'max'],
            'humedad': ['mean', 'min', 'max']
        }).round(2)
        
        # Renombrar columnas para mejor legibilidad
        estadisticas_por_ciudad.columns = [
            'temperatura_promedio', 'temperatura_minima', 'temperatura_maxima',
            'humedad_promedio', 'humedad_minima', 'humedad_maxima'
        ]
        
        print("\nEstadísticas de temperatura y humedad por ciudad:")
        print(estadisticas_por_ciudad.to_markdown())
        
        # Identificar ciudades con temperaturas extremas
        ciudad_mas_caliente = df_filtrado.loc[df_filtrado['temperatura'].idxmax()]
        ciudad_mas_fria = df_filtrado.loc[df_filtrado['temperatura'].idxmin()]
        ciudad_mas_humeda = df_filtrado.loc[df_filtrado['humedad'].idxmax()]
        ciudad_menos_humeda = df_filtrado.loc[df_filtrado['humedad'].idxmin()]
        
        print("\n--- Ciudades con Condiciones Extremas ---")
        print(f"Ciudad más cálida: {ciudad_mas_caliente['ciudad']}")
        print(f"Temperatura: {ciudad_mas_caliente['temperatura']:.2f}°C")
        print(f"Humedad: {ciudad_mas_caliente['humedad']}%")
        
        print(f"\nCiudad más fría: {ciudad_mas_fria['ciudad']}")
        print(f"Temperatura: {ciudad_mas_fria['temperatura']:.2f}°C")
        print(f"Humedad: {ciudad_mas_fria['humedad']}%")
        
        print(f"\nCiudad más húmeda: {ciudad_mas_humeda['ciudad']}")
        print(f"Humedad: {ciudad_mas_humeda['humedad']}%")
        print(f"Temperatura: {ciudad_mas_humeda['temperatura']:.2f}°C")
        
        print(f"\nCiudad menos húmeda: {ciudad_menos_humeda['ciudad']}")
        print(f"Humedad: {ciudad_menos_humeda['humedad']}%")
        print(f"Temperatura: {ciudad_menos_humeda['temperatura']:.2f}°C")
        
        # Crear visualizaciones
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Gráfico de temperaturas
        sns.barplot(x='ciudad', y='temperatura', data=df_filtrado, ax=ax1, palette='coolwarm')
        ax1.set_title('Temperaturas por Ciudad', fontsize=14, pad=20)
        ax1.set_xlabel('Ciudad', fontsize=12)
        ax1.set_ylabel('Temperatura (°C)', fontsize=12)
        ax1.tick_params(axis='x', rotation=45)
        
        # Añadir etiquetas de temperatura
        for i, v in enumerate(df_filtrado['temperatura']):
            ax1.text(i, v + 0.5, f'{v:.1f}°C', ha='center')
        
        # Gráfico de humedad
        sns.barplot(x='ciudad', y='humedad', data=df_filtrado, ax=ax2, palette='Blues')
        ax2.set_title('Humedad por Ciudad', fontsize=14, pad=20)
        ax2.set_xlabel('Ciudad', fontsize=12)
        ax2.set_ylabel('Humedad (%)', fontsize=12)
        ax2.tick_params(axis='x', rotation=45)
        
        # Añadir etiquetas de humedad
        for i, v in enumerate(df_filtrado['humedad']):
            ax2.text(i, v + 1, f'{v}%', ha='center')
        
        plt.tight_layout()
        plt.show()
        print("\nGráficos de temperatura y humedad generados.")
        
        # Análisis de correlación
        print("\n--- Análisis de Correlación ---")
        correlacion = df_filtrado[['temperatura', 'humedad']].corr().round(2)
        print("\nCorrelación entre temperatura y humedad:")
        print(correlacion.to_markdown())
        
        # Crear gráfico de dispersión
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df_filtrado, x='temperatura', y='humedad', hue='ciudad', s=100)
        plt.title('Relación entre Temperatura y Humedad por Ciudad', fontsize=14, pad=20)
        plt.xlabel('Temperatura (°C)', fontsize=12)
        plt.ylabel('Humedad (%)', fontsize=12)
        
        # Añadir etiquetas de ciudad
        for i, row in df_filtrado.iterrows():
            plt.text(row['temperatura'], row['humedad'], row['ciudad'], 
                    fontsize=9, ha='center', va='bottom')
        
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()
        print("\nGráfico de dispersión temperatura vs humedad generado.")

    except Exception as e:
        print(f"ERROR: Ocurrió un error durante el análisis avanzado: {str(e)}")
        print("\nInformación de depuración:")
        print(f"Connection string: {MONGO_CONNECTION_STRING[:20]}...")  # Solo mostramos el inicio por seguridad
    finally:
        if 'client' in locals():
            client.close()
            print("\nConexión a MongoDB cerrada.")
