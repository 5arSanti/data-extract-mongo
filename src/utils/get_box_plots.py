import matplotlib.pyplot as plt
import seaborn as sns

from config.config import (
    UNITS_SYMBOL
)


def get_box_plots(df_clima):
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
