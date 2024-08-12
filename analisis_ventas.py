from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# Datos de conexión
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
ENDPOINT = '127.0.0.1'  # Cambia según tu configuración
USER = 'postgres'
PASSWORD = ''
PORT = 5432  # Puerto por defecto de PostgreSQL
DATABASE = ''

# Conexión a la base de datos
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

try:
    connection = engine.connect()
    print("Conexión exitosa")
    connection.close()
except Exception as e:
    print(f"Error al conectar: {e}")

# Lista de meses y ruta de los archivos Excel
meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO']
ruta = r"C:\Users\fcobeltran\Documents\VENTAS 2024\VENTAS 2024"

# Importar datos desde Excel a PostgreSQL
for mes in meses:
    archivo_excel = os.path.join(ruta, f"{mes}.xlsx")
    df = pd.read_excel(archivo_excel, sheet_name='Ventas', header=3, usecols="A,C,I,J,M")

    df.columns = ['id', 'fecha_creacion', 'tipo_venta', 'personas', 'total_venta']

    df['fecha_creacion'] = pd.to_datetime(df['fecha_creacion'], format='%d-%m-%Y %H:%M:%S')
    df['total_venta'] = pd.to_numeric(df['total_venta'], errors='coerce')

    df.to_sql(mes.lower(), engine, if_exists='replace', index=False)

    print(f"Datos de {mes} importados con éxito.")

# Análisis Semanal
query_semanas = """
SELECT 
    EXTRACT(WEEK FROM fecha_creacion) AS semana, 
    SUM(total_venta) AS total_ventas
FROM 
    {mes}
GROUP BY 
    EXTRACT(WEEK FROM fecha_creacion)
ORDER BY 
    total_ventas DESC;
"""

for mes in meses:
    df_semanal = pd.read_sql(query_semanas.format(mes=mes.lower()), engine)
    print(f"Análisis Semanal de {mes}:")
    print(df_semanal)

# Análisis Horario
query_horas = """
SELECT 
    EXTRACT(HOUR FROM fecha_creacion) AS hora, 
    SUM(total_venta) AS total_ventas
FROM 
    {mes}
GROUP BY 
    EXTRACT(HOUR FROM fecha_creacion)
ORDER BY 
    hora;
"""

horas_resultados = []

for mes in meses:
    df_horas = pd.read_sql(query_horas.format(mes=mes.lower()), engine)
    df_horas['Mes'] = mes
    horas_resultados.append(df_horas)
    print(f"Análisis Horario de {mes}:")
    print(df_horas)

# Combinar todos los resultados en un solo DataFrame
df_horas_resultados = pd.concat(horas_resultados, ignore_index=True)

# Gráfico de Consumo por Hora en Cada Mes
plt.figure(figsize=(14, 8))

for mes in meses:
    df_mes = df_horas_resultados[df_horas_resultados['Mes'] == mes]
    plt.plot(df_mes['hora'], df_mes['total_ventas'], marker='o', label=mes)

plt.title('Consumo Total por Hora en Cada Mes')
plt.xlabel('Hora del Día')
plt.ylabel('Total Ventas (CLP)')
plt.legend(title='Mes')
plt.grid(True)
plt.xticks(range(0, 24))
plt.show()

# Análisis de Consumo Promedio por Persona
query_consumo = """
SELECT 
    AVG(total_venta/personas) AS consumo_promedio
FROM 
    {mes}
WHERE 
    personas > 0;
"""

resultados = []

for mes in meses:
    df_consumo = pd.read_sql(query_consumo.format(mes=mes.lower()), engine)
    consumo_promedio = df_consumo['consumo_promedio'].iloc[0]
    resultados.append({'Mes': mes, 'Consumo Promedio': consumo_promedio})
    print(f"Consumo Promedio por Persona en {mes}:")
    print(df_consumo)

# Convertir los resultados en un DataFrame
df_resultados = pd.DataFrame(resultados)

# Gráfico de Consumo Promedio por Persona en Cada Mes
plt.figure(figsize=(10, 6))
plt.bar(df_resultados['Mes'], df_resultados['Consumo Promedio'], color='skyblue')
plt.title('Consumo Promedio por Persona en Cada Mes')
plt.xlabel('Mes')
plt.ylabel('Consumo Promedio (CLP)')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()

# Gráfico de Consumo Promedio por Persona (Línea)
plt.figure(figsize=(10, 6))
plt.plot(df_resultados['Mes'], df_resultados['Consumo Promedio'], marker='o', linestyle='-', color='blue')
plt.title('Consumo Promedio por Persona en Cada Mes')
plt.xlabel('Mes')
plt.ylabel('Consumo Promedio (CLP)')
plt.grid(True)
plt.xticks(rotation=45)
plt.show()

# Análisis y Gráfico de Consumo Total por Hora (Desde el Inicio del Negocio)
query_horas_total = """
SELECT 
    EXTRACT(HOUR FROM fecha_creacion) AS hora, 
    SUM(total_venta) AS total_ventas
FROM 
    {mes}
GROUP BY 
    EXTRACT(HOUR FROM fecha_creacion)
ORDER BY 
    hora;
"""

df_horas_totales = pd.DataFrame()

for mes in meses:
    df_horas = pd.read_sql(query_horas_total.format(mes=mes.lower()), engine)
    df_horas_totales = pd.concat([df_horas_totales, df_horas], ignore_index=True)

df_horas_agrupadas = df_horas_totales.groupby('hora')['total_ventas'].sum().reset_index()

df_horas_ordenadas = df_horas_agrupadas.sort_values(by='total_ventas', ascending=False).reset_index(drop=True)

plt.figure(figsize=(12, 6))
plt.bar(df_horas_ordenadas['hora'].astype(str) + ":00", df_horas_ordenadas['total_ventas'], color='skyblue')
plt.title('Consumo Total por Hora (Desde el Inicio del Negocio)')
plt.xlabel('Hora del Día')
plt.ylabel('Total Ventas (CLP)')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()

# Análisis de Volumen de Ventas y Línea de Tendencia
query_volumen = """
SELECT 
    SUM(total_venta) AS total_ventas
FROM 
    {mes};
"""

volumen_ventas = []

for mes in meses:
    df_volumen = pd.read_sql(query_volumen.format(mes=mes.lower()), engine)
    volumen_ventas.append(df_volumen['total_ventas'].iloc[0])

df_volumen_ventas = pd.DataFrame({'Mes': meses, 'Total Ventas': volumen_ventas})

plt.figure(figsize=(10, 6))
plt.bar(df_volumen_ventas['Mes'], df_volumen_ventas['Total Ventas'], color='skyblue', label='Volumen de Ventas')

x = np.arange(len(df_volumen_ventas))
z = np.polyfit(x, df_volumen_ventas['Total Ventas'], 1)
p = np.poly1d(z)

plt.plot(df_volumen_ventas['Mes'], p(x), color='red', linestyle='--', label='Línea de Tendencia')

plt.title('Volumen de Ventas y Línea de Tendencia')
plt.xlabel('Mes')
plt.ylabel('Total Ventas (CLP)')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.show()

