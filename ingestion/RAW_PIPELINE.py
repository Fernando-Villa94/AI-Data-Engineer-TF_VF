# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Importar las funciones necesarias de PySpark
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as f
from io import StringIO
import sys

def get_data(path):
    print("Cargando sales...")
    df_sales_landing = spark.read.parquet(path + "sales.parquet")
    df_sales_raw = df_sales_landing.select("*", f.current_timestamp().alias("fecha_registro"))

    print("Cargando clientes...")
    df_customers_landing = spark.read \
        .option("header", "true") \
        .csv(path + "customers.csv")
    df_customers_raw = df_customers_landing.select("*", f.current_timestamp().alias("fecha_registro"))

    print("Cargando Catálogo de Productos...")
    df_products_landing = spark.read \
        .option("multiLine", "true") \
        .json(path + "products.json")
    df_products_raw = df_products_landing.select("*", f.current_timestamp().alias("fecha_registro"))

        # Validaciones de conteo
    try:
        count_sales = df_sales_raw.count()
        print(f"Cantidad de registros cargados en sales: {count_sales}")
        if count_sales == 0: print("ALERTA DE CALIDAD: El dataset 'sales' está vacío.")
    except Exception as e:
        print(f" Error inesperado en lectura de tabla sales: {e}")
        
    try:
        count_customers = df_customers_raw.count()
        print(f"Cantidad de registros cargados en clientes: {count_customers}")
        if count_customers == 0: print("ALERTA DE CALIDAD: El dataset 'clientes' está vacío.")
    except Exception as e:
        print(f" Error inesperado en lectura de tabla clientes: {e}")
        
    try:
        count_products = df_products_raw.count()
        print(f"Cantidad de registros cargados en productos: {count_products}")
        if count_products == 0: print("ALERTA DE CALIDAD: El dataset 'productos' está vacío.")
    except Exception as e:
        print(f" Error inesperado en lectura de tabla productos: {e}")  

    return df_sales_raw, df_customers_raw, df_products_raw


def validar_y_castear_esquema(df_fuente, esquema_destino, nombre_tabla):
    """
    Compara el esquema inferido de la fuente contra el esquema existente en la capa RAW.
    Si coinciden, aplica el casteo a string (excepto fecha_registro) y retorna el DataFrame.
    """
    tabla_completa = f"{esquema_destino}.{nombre_tabla}"
    
    # 1. Intentar leer la tabla RAW actual para comparar esquemas
    if spark.catalog.tableExists(tabla_completa):
        print(f" Validando esquema para {tabla_completa} contra la tabla existente...")
        df_raw_existente = spark.read.table(tabla_completa)
        
        # Comparamos nombres de columnas (ignorando el orden)
        columnas_fuente = set(df_fuente.columns)
        columnas_destino = set(df_raw_existente.columns)
        
        if columnas_fuente != columnas_destino:
            faltan_en_fuente = columnas_destino - columnas_fuente
            sobran_en_fuente = columnas_fuente - columnas_destino
            error_msg = f" ERROR DE CALIDAD: El esquema de la fuente no coincide con {tabla_completa}."
            if faltan_en_fuente: error_msg += f" Faltan columnas: {faltan_en_fuente}."
            if sobran_en_fuente: error_msg += f" Columnas extra no esperadas: {sobran_en_fuente}."
            raise ValueError(error_msg)
        
        print(f"Esquema validado con éxito para {nombre_tabla}.")
    else:
        print(f"La tabla {tabla_completa} no existe en RAW. Se creará por primera vez.")

    # 2. Proceder con la conversión de tipos (Castear a string EXCEPTO 'fecha_registro')
    columnas_casteadas = [
        f.col(c).cast("string").alias(c) if c != "fecha_registro" else f.col(c) 
        for c in df_fuente.columns
    ]
    return df_fuente.select(columnas_casteadas)


def write_raw_data(esquema_destino, df_sales_raw, df_customers_raw, df_products_raw):
    """
    Escribe las tablas en la capa RAW utilizando formato Delta sin sobreescribir el esquema de forma forzada.
    """
    # Validar esquemas y realizar conversiones antes de escribir
    df_sales_final = validar_y_castear_esquema(df_sales_raw, esquema_destino, "sales")
    df_customers_final = validar_y_castear_esquema(df_customers_raw, esquema_destino, "customers")
    df_products_final = validar_y_castear_esquema(df_products_raw, esquema_destino, "products")

    print("\nEscribiendo Ventas en formato Delta...")
    df_sales_final.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{esquema_destino}.sales")

    print("Escribiendo Clientes en formato Delta...")
    df_customers_final.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{esquema_destino}.customers")

    print("Escribiendo Productos en formato Delta...")
    df_products_final.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{esquema_destino}.products")


# COMMAND ----------

def main(): 
    input_path = "/Volumes/Workspace/default/data_tf_ai_de/"
    esquema_destino = "catalog_raw"
    
    # Flujo de ejecución
    df_sales_raw, df_customers_raw, df_products_raw = get_data(input_path)
    write_raw_data(esquema_destino, df_sales_raw, df_customers_raw, df_products_raw)


if __name__ == "__main__":
    main()

