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

CITIES = ['Bogota', 'Medellin', 'Cali']
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
