# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "dbt-databricks",
# ]
# ///
# MAGIC %pip install dbt-databricks  

# COMMAND ----------

import os
from dbt.cli.main import dbtRunner
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from dateutil.relativedelta import relativedelta
from datetime import datetime

# COMMAND ----------

ruta_proyecto_dbt = "/Workspace/Repos/fvilla280494@gmail.com/AI-Data-Engineer-TF_VF/dbt"
os.environ["DBT_ACCESS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

# MAGIC %md
# MAGIC Staging

# COMMAND ----------

dbt = dbtRunner()
argumentos_dbt = [
    "build", 
    "--select", "staging",       # <-- Corre TODO lo que esté dentro de models/staging/
    "--project-dir", ruta_proyecto_dbt,
    "--profiles-dir", ruta_proyecto_dbt,
]

resultado_dbt = dbt.invoke(argumentos_dbt)

# COMMAND ----------

# MAGIC %md
# MAGIC Intermediate

# COMMAND ----------

dbt = dbtRunner()
argumentos_dbt = [
    "build", 
    "--select", "intermediate",      
    "--project-dir", ruta_proyecto_dbt,
    "--profiles-dir", ruta_proyecto_dbt,    
]


resultado_dbt = dbt.invoke(argumentos_dbt)

# COMMAND ----------

# MAGIC %md
# MAGIC Marts

# COMMAND ----------

dbt = dbtRunner()

tabla_intermedia = "workspace.catalog_intermediate.int_customer_monthly_aggregates"
tabla_historica_final = "workspace.catalog_marts.customer_360_historico"

# ---  NUEVA LÓGICA: OBTENER EL ÚLTIMO CODMES DE LA TABLA INTERMEDIA ---
print(f" Identificando el último periodo disponible en {tabla_intermedia}...")

# Obtenemos el valor máximo de codmes
max_codmes_row = spark.table(tabla_intermedia).select(F.max("codmes")).collect()[0][0]

if max_codmes_row is None:
    raise Exception(f" La tabla {tabla_intermedia} está vacía. No se puede calcular la ventana.")

print(f" Último codmes detectado: {max_codmes_row}")

# Convertimos el entero YYYYMM (Ej: 202602) a un objeto datetime para operar meses fácilmente
ultimo_anio = max_codmes_row // 100
ultimo_mes = max_codmes_row % 100
fecha_base = datetime(ultimo_anio, ultimo_mes, 1)

# Generamos la lista de los últimos  meses a partir de la fecha base
meses_a_procesar = []

dbutils.widgets.text("meses_back", "6", "Meses a procesar hacia atrás")

# Leer el valor del widget y convertirlo a entero de forma segura
try:
    meses_back = int(dbutils.widgets.get("meses_back"))
    print(f" Parámetro recibido: Se procesarán {meses_back} meses.")
except ValueError:
    print(" El valor del parámetro 'meses_back' no es un número válido. Se usará el valor por defecto: 6")
    meses_back = 6

for i in range(meses_back): 
    fecha_periodo = fecha_base - relativedelta(months=i)
    periodo_int = int(fecha_periodo.strftime("%Y%m"))
    meses_a_procesar.append(periodo_int)

# Ordenamos de menor a mayor (del mes más antiguo al más reciente)
meses_a_procesar.sort()
# ------------------------------------------------------------------------

print(f" Periodos dinámicos a procesar (Ventana móvil de {meses_back} meses): {meses_a_procesar}")
print(" Iniciando procesamiento masivo histórico vía dbtRunner...")

print("Ejecutando dim_products")
argumentos_dbt = [
    "build", 
    "--select", "dim_products", 
    "--project-dir", ruta_proyecto_dbt,
    "--profiles-dir", ruta_proyecto_dbt,

]
resultado_dbt = dbt.invoke(argumentos_dbt)
if not resultado_dbt.success:
    print(f" Error detallado de dbt: {resultado_dbt.exception}")
    raise Exception(f" dbt falló al procesar products")

print("Ejecutando dim_customers")
argumentos_dbt = [
    "build", 
    "--select", "dim_customers", 
    "--project-dir", ruta_proyecto_dbt,
    "--profiles-dir", ruta_proyecto_dbt,

]
resultado_dbt = dbt.invoke(argumentos_dbt)
if not resultado_dbt.success:
    print(f" Error detallado de dbt: {resultado_dbt.exception}")
    raise Exception(f" dbt falló al procesar customers")

print("Ejecutando fct_Sales")

argumentos_dbt = [
    "build", 
    "--select", "fct_sales", 
    "--project-dir", ruta_proyecto_dbt,
    "--profiles-dir", ruta_proyecto_dbt,

]
resultado_dbt = dbt.invoke(argumentos_dbt)
if not resultado_dbt.success:
    print(f" Error detallado de dbt: {resultado_dbt.exception}")
    raise Exception(f" dbt falló al procesar tabla de hechos")

print(f" dbt terminó con éxito las dimensiones y hechos")

for mes in meses_a_procesar:
    print(f"\n--------------------------------------------------")
    print(f" 1. Invocando dbt para el periodo: {mes}")
    print(f"--------------------------------------------------")
    
    argumentos_dbt = [
        "build", 
        "--select", "customer_360", 
        "--project-dir", ruta_proyecto_dbt,
        "--profiles-dir", ruta_proyecto_dbt,
        "--vars", f"{{'codmes_inferencia': {mes}}}"
    ]
    
    # Ejecutamos dbt internamente en la memoria de Python
    resultado_dbt = dbt.invoke(argumentos_dbt)
    
    # Validamos si dbt falló
    if not resultado_dbt.success:
        print(f" Error detallado de dbt: {resultado_dbt.exception}")
        raise Exception(f" dbt falló al procesar el mes {mes}. Deteniendo el bucle.")
        
    print(f" dbt terminó con éxito. Leyendo vista generada para {mes}...")
    
    # 2. Recuperamos el lote en Spark
    df_lote_mes = spark.sql("SELECT * FROM workspace.catalog_marts.customer_360")

    if spark.catalog.tableExists(tabla_historica_final):
        print(f" Aplicando MERGE (Upsert) para el periodo {mes}...")
        
        # Referenciamos la tabla histórica Delta existente
        tabla_delta_historica = DeltaTable.forName(spark, tabla_historica_final)
        
        # Ejecutamos el Merge
        tabla_delta_historica.alias("target") \
            .merge(
                df_lote_mes.alias("source"),
                "target.periodo_analisis = source.periodo_analisis AND target.customer_id = source.customer_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
            
    else:
        # 3. Primera ejecución: Si la tabla no existe, la creamos con un simple write
        print(f" La tabla histórica no existe. Creándola por primera vez con el periodo {mes}...")
        df_lote_mes.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(tabla_historica_final)
        
    print(f" Procesamiento del periodo {mes} guardado correctamente.")

print("\n ¡Procesamiento histórico completado exitosamente sin duplicados!")