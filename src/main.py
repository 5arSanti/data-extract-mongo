import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, PyMongoError
import time
from datetime import datetime
import tabulate
import json

API_KEY = '3be2c91d3c61bcc3f81b8c701fe1fbf5'
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'

CITIES = ['Bogota', 'Medellin', 'Cali', 'London', 'New York', 'Sydney', 'Cairo', 'Rio de Janeiro']
UNITS = 'metric'
LANG = 'es'

all_weather_data = []

print("--- Iniciando proceso de extracción de datos meteorológicos ---")

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


df_clima = pd.DataFrame(all_weather_data)

print("\n--- Vista previa del DataFrame de Clima (primeras 5 filas) ---")
print(df_clima.head().to_markdown(index=False))

print("\n--- Información Concisa del DataFrame (Tipos de datos y valores no nulos) ---")
print(df_clima.info())

print("\n--- Resumen Estadístico Descriptivo del DataFrame ---")
print(df_clima.describe(include='all').to_markdown())


print("\n--- Iniciando Procesamiento y Análisis Avanzado de Datos ---")

UNITS_SYMBOL = 'C' if UNITS == 'metric' else 'F'

print("\nConteos de valores nulos por columna (antes de la limpieza profunda):")
print(df_clima.isnull().sum().to_markdown())

if df_clima.empty:
    print("El DataFrame está vacío, no hay datos para procesar.")
else:
    initial_rows = df_clima.shape[0]
    df_clima.dropna(subset=['temperatura', 'ciudad', 'descripcion_clima'], inplace=True)
    rows_after_dropna = df_clima.shape[0]
    if initial_rows > rows_after_dropna:
        print(f"Se eliminaron {initial_rows - rows_after_dropna} filas con valores nulos en columnas cruciales.")
    else:
        print("No se encontraron filas con valores nulos en columnas cruciales para eliminar.")

    print("\nConteos de valores nulos por columna (después de la limpieza):")
    print(df_clima.isnull().sum().to_markdown())

    for col in ['temperatura', 'sensacion_termica', 'temp_min', 'temp_max', 'humedad', 'presion', 'viento_velocidad', 'nubes_porcentaje']:
        df_clima[col] = pd.to_numeric(df_clima[col], errors='coerce')
    df_clima.dropna(subset=['temperatura'], inplace=True)

    try:
        df_clima['dia_semana'] = df_clima['fecha_hora_consulta_utc'].dt.day_name(locale='es_ES.UTF-8')
    except AttributeError:
        df_clima['dia_semana'] = df_clima['fecha_hora_consulta_utc'].dt.day_name()
        print("Advertencia: No se pudo configurar el locale 'es'. Los nombres de los días están en inglés.")

    def categorize_temperature(temp):
        if temp < 10:
            return 'Frío Extremo'
        elif 10 <= temp < 18:
            return 'Frío'
        elif 18 <= temp < 25:
            return 'Templado'
        elif 25 <= temp < 30:
            return 'Cálido'
        else:
            return 'Calor Extremo'

    df_clima['categoria_temperatura'] = df_clima['temperatura'].apply(categorize_temperature)

    print(f"\nNúmero de filas duplicadas (antes de eliminar): {df_clima.duplicated().sum()}")
    df_clima.drop_duplicates(inplace=True)
    print(f"Número de filas después de eliminar duplicados: {df_clima.shape[0]}")


    print("\n--- Ejecutando Análisis Exploratorio de Datos (EDA) ---")

    print("\nEstadísticas descriptivas de la humedad:")
    print(df_clima['humedad'].describe().to_markdown())

    print("\nConteo de ciudades únicas y sus ocurrencias:")
    print(df_clima['ciudad'].value_counts().to_markdown())

    print("\nDistribución de categorías de temperatura:")
    print(df_clima['categoria_temperatura'].value_counts().to_markdown())


    print("\nResumen de temperaturas y humedad por ciudad (promedio, min, max, desviación estándar):")
    resumen_por_ciudad = df_clima.groupby('ciudad').agg(
        temperatura_media=('temperatura', 'mean'),
        temperatura_minima=('temperatura', 'min'),
        temperatura_maxima=('temperatura', 'max'),
        humedad_media=('humedad', 'mean'),
        humedad_std=('humedad', 'std')
    ).round(2)
    print(resumen_por_ciudad.to_markdown())

    ciudad_mas_caliente = df_clima.loc[df_clima['temperatura'].idxmax()]
    print(f"\nCiudad con la temperatura actual más alta: {ciudad_mas_caliente['ciudad']} ({ciudad_mas_caliente['temperatura']:.2f} {UNITS_SYMBOL}°)")

    ciudad_mas_fria = df_clima.loc[df_clima['temperatura'].idxmin()]
    print(f"Ciudad con la temperatura actual más baja: {ciudad_mas_fria['ciudad']} ({ciudad_mas_fria['temperatura']:.2f} {UNITS_SYMBOL}°)")

    ciudad_mas_humeda = df_clima.loc[df_clima['humedad'].idxmax()]
    print(f"Ciudad con la humedad actual más alta: {ciudad_mas_humeda['ciudad']} ({ciudad_mas_humeda['humedad']:.2f} %)")

    print("\n--- Conclusión del Análisis Detallado de Datos ---")
    print("El procesamiento de datos ha permitido transformar los datos crudos de la API en un formato estructurado y enriquecido, apto para análisis.")
    print("Se han identificado patrones de temperatura y humedad, así como la distribución por categorías climáticas.")
    print("El análisis agrupado por ciudad ha proporcionado una visión exhaustiva de las condiciones promedio y las variaciones dentro de cada ubicación.")
    print("Estos hallazgos son fundamentales para la visualización y la posterior toma de decisiones o integración en sistemas.")

    print("\n--- Vista previa del DataFrame después de todo el procesamiento ---")
    print(df_clima.head().to_markdown(index=False))


print("\n--- Generando Visualizaciones de Datos ---")

if df_clima.empty:
    print("El DataFrame está vacío, no se pueden generar gráficos.")
else:
    sns.set_style("whitegrid")
    sns.set_palette("viridis")

    plt.figure(figsize=(10, 6))
    sns.barplot(x='ciudad', y='temperatura', data=df_clima,
                palette='viridis', edgecolor='black', zorder=2)

    for index, row in df_clima.iterrows():
        plt.text(index, row['temperatura'] + 0.5, f"{row['temperatura']:.1f}{UNITS_SYMBOL}°",
                 color='black', ha="center", va='bottom', fontsize=9)

    plt.title(f'Temperatura Actual por Ciudad ({UNITS_SYMBOL}°)', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Ciudad', fontsize=12)
    plt.ylabel(f'Temperatura Actual ({UNITS_SYMBOL}°)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
    print("Gráfico de Temperaturas por Ciudad generado.")

    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='humedad', y='temperatura', hue='ciudad', data=df_clima,
                    s=100, alpha=0.8, edgecolor='w')
    plt.title(f'Relación entre Temperatura y Humedad por Ciudad ({UNITS_SYMBOL}°)', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Humedad (%)', fontsize=12)
    plt.ylabel(f'Temperatura ({UNITS_SYMBOL}°)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(title='Ciudad', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()
    print("Gráfico de Dispersión (Temperatura vs. Humedad) generado.")

    plt.figure(figsize=(8, 5))
    sns.histplot(df_clima['temperatura'], bins=5, kde=True, color='skyblue', edgecolor='black')
    plt.title(f'Distribución de Temperaturas ({UNITS_SYMBOL}°)', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel(f'Temperatura ({UNITS_SYMBOL}°)', fontsize=12)
    plt.ylabel('Frecuencia', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
    print("Histograma de Temperaturas generado.")

    plt.figure(figsize=(10, 6))
    sns.boxplot(x='categoria_temperatura', y='temperatura', data=df_clima,
                palette='coolwarm', order=['Frío Extremo', 'Frío', 'Templado', 'Cálido', 'Calor Extremo'])
    plt.title(f'Distribución de Temperatura por Categoría ({UNITS_SYMBOL}°)', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Categoría de Temperatura', fontsize=12)
    plt.ylabel(f'Temperatura ({UNITS_SYMBOL}°)', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
    print("Box Plot de Temperaturas por Categoría generado.")

print("\n--- Proceso de visualización de datos completado ---")


print("\n--- Iniciando Almacenamiento y Operaciones Avanzadas en MongoDB ---")

MONGO_CONNECTION_STRING = "mongodb+srv://johelsariasb:u3znwTHJEAJhgJmt@data-extract-mongo.a3o9uaa.mongodb.net/?retryWrites=true&w=majority&appName=data-extract-mongo"

if df_clima.empty:
    print("El DataFrame está vacío. No hay datos para insertar o manipular en MongoDB.")
else:
    try:
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)


        client.admin.command('ping')
        print("Conexión exitosa a MongoDB.")

        db = client["clima"]
        coleccion = db["datos_meteorologicos"]

        datos_para_mongo = df_clima.to_dict("records")

        print(f"\nBorrando documentos existentes en la colección '{coleccion.name}' para una inserción fresca...")
        delete_result = coleccion.delete_many({})
        print(f"Documentos eliminados: {delete_result.deleted_count}")

        print(f"Insertando {len(datos_para_mongo)} documentos en la colección '{coleccion.name}'...")
        insert_result = coleccion.insert_many(datos_para_mongo)
        print(f"Número de documentos insertados: {len(insert_result.inserted_ids)}.")

        print("\n--- Verificando datos en MongoDB con consultas avanzadas ---")

        print("\nDocumentos con las 5 temperaturas más altas (ciudad y temperatura):")
        for doc in coleccion.find({}, {'ciudad': 1, 'temperatura': 1, '_id': 0}).sort('temperatura', -1).limit(5):
            print(doc)

        print("\nTodos los documentos de 'Medellin':")
        for doc in coleccion.find({'ciudad': 'Medellin'}):
            import json
            print(json.dumps(doc, indent=2, default=str))

        print("\nDocumentos con humedad superior al 75% (ciudad y humedad):")
        for doc in coleccion.find({'humedad': {'$gt': 75}}, {'ciudad': 1, 'humedad': 1, '_id': 0}):
            print(doc)

        print("\n--- Actualizando un documento en MongoDB ---")
        update_query = {'ciudad': 'Bogota'}
        new_values = {'$set': {'nota_especial': 'Condiciones monitoreadas'}}
        update_result = coleccion.update_one(update_query, new_values)
        print(f"Documentos actualizados: {update_result.modified_count}")
        print("Documento de 'Bogota' después de la actualización:")
        print(json.dumps(coleccion.find_one({'ciudad': 'Bogota'}), indent=2, default=str))

        client.close()
        print("\nConexión a MongoDB cerrada exitosamente.")

    except ServerSelectionTimeoutError as err:
        print(f"ERROR: No se pudo conectar a MongoDB. Tiempo de espera excedido. Detalles: {err}")
        print("Verifique la accesibilidad de su servidor MongoDB:")
        print("1. Si es local, asegúrese de que el servicio `mongod` esté en ejecución y accesible en la dirección y puerto especificados (usualmente `localhost:27017`).")
        print("2. Si usa MongoDB Atlas, revise que su `MONGO_CONNECTION_STRING` sea correcta (usuario, contraseña, URL del clúster) y que su dirección IP esté permitida en 'Network Access' en el panel de Atlas.")
        print("3. Compruebe posibles configuraciones de firewall que puedan estar bloqueando la conexión (el puerto 27017 o 27017-27019 para Atlas).")
    except ConnectionFailure as err:
        print(f"ERROR: Fallo en la conexión a MongoDB. Detalles: {err}")
    except PyMongoError as err:
        print(f"ERROR: Un error específico de PyMongo ocurrió: {err}")
    except Exception as e:
        print(f"ERROR: Ocurrió un error inesperado al conectar o almacenar datos en MongoDB: {e}")

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
