# Databricks notebook source
# Importar las funciones necesarias de PySpark
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as f


from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as f

def get_data(path):

    print("Cargando sales")
    df_sales_landing = spark.read.parquet(path + "sales.parquet")
    df_sales_raw = df_sales_landing.select("*", f.current_timestamp().alias("fecha_registro"))

    print("Cargando clientes")
    df_customers_landing = spark.read \
        .option("header", "true") \
        .csv(path + "customers.csv")
    df_customers_raw = df_customers_landing.select("*", f.current_timestamp().alias("fecha_registro"))

    print("Cargando Catálogo de Productos ")
    df_products_landing = spark.read \
        .option("multiLine", "true") \
        .json(path + "products.json")
    df_products_raw = df_products_landing.select("*", f.current_timestamp().alias("fecha_registro"))

    # Validaciones de conteo (¡Ojo! Tenías cru2zados los nombres en tus prints, aquí ya están corregidos)
    try:
        print(f"Cantidad de registros cargados en sales: {df_sales_raw.count()}")
    except Exception as e:
        print(f" Error inesperado en lectura de tabla sales: {e}")
        
    try:
        print(f"Cantidad de registros cargados en clientes: {df_customers_raw.count()}")
    except Exception as e:
        print(f" Error inesperado en lectura de tabla clientes: {e}")
        
    try:
        print(f"Cantidad de registros cargados en productos: {df_products_raw.count()}")
    except Exception as e:
        print(f" Error inesperado en lectura de tabla productos: {e}")  

    # MEJORA: Función interna para castear todo a string EXCEPTO 'fecha_registro'
    def cast_all_to_string(df):
        if df is None: return None
        # Mantiene 'fecha_registro' con su tipo original y convierte el resto a string
        columnas = [
            f.col(c).cast("string").alias(c) if c != "fecha_registro" else f.col(c) 
            for c in df.columns
        ]
        return df.select(columnas)

    # Aplicamos el casteo y reasignamos explícitamente las variables
    df_sales_raw = cast_all_to_string(df_sales_raw)
    df_customers_raw = cast_all_to_string(df_customers_raw)
    df_products_raw = cast_all_to_string(df_products_raw)

    return df_sales_raw, df_customers_raw, df_products_raw

def write_raw_data(esquema_destino, df_sales_raw, df_customers_raw, df_products_raw):
    """
    Escribe las tablas en la capa RAW utilizando formato Delta y registrándolas en el Catálogo.
    esquema_destino: Debe ser el nombre de tu esquema (ej. "workspace.catalog_raw")
    """
    print("Escribiendo Ventas en formato Delta...")
    df_sales_raw.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{esquema_destino}.sales")

    print("Escribiendo Clientes en formato Delta...")
    df_customers_raw.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{esquema_destino}.customers")

    print("Escribiendo Productos en formato Delta...")
    df_products_raw.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{esquema_destino}.products")


# COMMAND ----------

def main(): 
    
    df_sales_raw,df_customers_raw,df_products_raw=get_data("/Volumes/Workspace/default/data_tf_ai_de/")
    write_raw_data("catalog_raw", df_sales_raw, df_customers_raw, df_products_raw)


if __name__ == "__main__":
    main()