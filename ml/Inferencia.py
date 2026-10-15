# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "lightgbm",
# ]
# ///
# MAGIC %pip install lightgbm

# COMMAND ----------

# MAGIC %md
# MAGIC Target

# COMMAND ----------

from pyspark.sql import functions as F

customer_360 = spark.table("workspace.catalog_marts.customer_360_historico")
monthly_sales = spark.table("workspace.catalog_intermediate.int_customer_monthly_aggregates")

customer_360 = (
    customer_360
    .select("*",F.to_date(
            F.concat(
                F.col("periodo_analisis").cast("string"),
                F.lit("01")
            ),
            "yyyyMMdd"
        ).alias("periodo_fecha")

    )
)

monthly_sales = (
    monthly_sales
    .select("*",        
        F.to_date(
            F.concat(
                F.col("codmes").cast("string"),
                F.lit("01")
            ),
            "yyyyMMdd"
        ).alias("codmes_fecha")
    )
)

max_fecha_ventas = (
    monthly_sales
    .agg(
        F.max("codmes_fecha").alias("max_fecha")
    )
    .collect()[0]["max_fecha"]
)

print(f"Última fecha disponible en ventas: {max_fecha_ventas}")

fecha_inicio_no_observable = F.add_months(
    F.lit(max_fecha_ventas),
    -5
)

print(
    f"Último mes disponible en ventas: {max_fecha_ventas}"
)


# =============================================================================
# 2. CALCULAR TARGET PARA LOS PERIODOS OBSERVABLES
# =============================================================================

customer_360_valid = (
    customer_360
    .filter(
        F.add_months(
            F.col("periodo_fecha"),
            6
        ) <= F.lit(max_fecha_ventas)
    )
)

target = (
    customer_360_valid.alias("c")
    .join(
        monthly_sales.alias("s"),
        (
            (F.col("c.customer_id") == F.col("s.customer_id"))
            &
            (F.col("s.codmes_fecha") > F.col("c.periodo_fecha"))
            &
            (
                F.col("s.codmes_fecha")
                <= F.add_months(
                    F.col("c.periodo_fecha"),
                    6
                )
            )
        ),
        "left"
    )
    .groupBy(
        F.col("c.customer_id"),
        F.col("c.periodo_analisis")
    )
    .agg(
        F.when(
            F.count(F.col("s.customer_id")) > 0,
            1
        )
        .otherwise(0)
        .cast("int")
        .alias("target_t")
    )
)



customer_360_final = (
    customer_360
    .drop("periodo_fecha")
    .join(
        target,
        on=[
            "customer_id",
            "periodo_analisis"
        ],
        how="left"
    )
    .select("*",
        F.when(
            F.col("periodo_analisis").cast("string")
            >= F.date_format(
                fecha_inicio_no_observable,
                "yyyyMM"
            ),
            F.lit(None).cast("int")
        ).otherwise(
            F.col("target_t")
        ).alias("target")
    ).drop("target_t")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Predicción

# COMMAND ----------

from pyspark.sql import functions as F
import mlflow
import mlflow.lightgbm

# =====================================================================
# 1. CARGAR MODELO DESDE MLFLOW Y PASARLO A UDF DE SPARK
# =====================================================================
# Reemplaza con tu RUN_ID obtenido en el paso anterior

dbutils.widgets.text("run_id", "")

# Leer el valor enviado por el Workflow (si no envían nada, puedes dejar uno por defecto)
run_id = dbutils.widgets.get("run_id")

if not run_id:
    raise ValueError("El parámetro 'run_id' es obligatorio para ejecutar este flujo automático.")

model_uri = f"runs:/{run_id}/model"
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")

# Cargamos el modelo como una función nativa de Spark (UDF)
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")

# Definimos las variables que el modelo espera exactamente en orden
variables_seleccionadas = [
    "monto_prom_6m", "frec_accessories_prom_6m", "recencia_dias",
    "yearly_income", "frec_clothing_prom_6m", "children_at_home",
    "total_children", "number_cars", "frec_bikes_prom_2m"
]


# Generamos el Score/Predicción usando la UDF de MLflow y la fecha de registro

df_predictions = customer_360_final.select("*",predict_udf(*[F.col(c) for c in variables_seleccionadas]).alias("prediction"),f.current_timestamp().alias("fecha_registro"))



# =====================================================================
# CONTROLES DE CALIDAD DE DATOS (PRE-ESCRITURA)
# =====================================================================
print("\n--- Iniciando controles de calidad de las predicciones ---")

# 1. Validación de registros totales cargados
total_registros = df_predictions.count()
print(f"📊 Total de registros procesados: {total_registros}")

if total_registros == 0:
    raise ValueError(" CONTROL DE CALIDAD FALLADO: El DataFrame de predicciones está completamente vacío.")

# 2. Prueba de No Nulidad en la columna 'prediction'
# Contamos cuántos registros tienen valores válidos (no nulos) en esa columna
registros_no_nulos = df_predictions.filter(F.col("prediction").isNotNull()).count()
print(f" Registros con predicción no nula: {registros_no_nulos} de {total_registros}")


# 3. Validación de No Duplicados en Llave Compuesta (customer_id + periodo_analisis)
# Agrupamos por la llave compuesta y contamos las ocurrencias de cada combinación
duplicados_df = df_predictions.groupBy("customer_id", "periodo_analisis") \
                              .count() \
                              .filter(F.col("count") > 1)

total_duplicados = duplicados_df.count()

if total_duplicados > 0:
    print(f"CONTROL DE CALIDAD FALLADO: Se encontraron {total_duplicados} combinaciones duplicadas de 'customer_id' y 'periodo_analisis'.")
    # Muestra  de los duplicados en los logs de Databricks para facilitar el debugging
    duplicados_df.show(5, truncate=False)
    raise ValueError("Error de unicidad: La llave compuesta (customer_id, periodo_analisis) contiene registros duplicados.")
else:
    print("Validación de unicidad exitosa: No existen llaves compuestas duplicadas.")

print("--- Todos los controles de calidad han sido aprobados. Procediendo a guardar los datos --- \n")

# COMMAND ----------

df_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("catalog_analytics.sales_customers_seguimiento")