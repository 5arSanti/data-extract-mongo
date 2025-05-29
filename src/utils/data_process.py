import pandas as pd

from config.config import (
    UNITS_SYMBOL
)

def data_process(df_clima):
    print("\n--- Iniciando Procesamiento y Análisis Avanzado de Datos ---")

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
