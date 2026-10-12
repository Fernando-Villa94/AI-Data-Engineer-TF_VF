# Databricks notebook source
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
run_id = "31c7e42081d149949f243264ee3375ea" 
model_uri = f"runs:/{run_id}/model"

# Cargamos el modelo como una función nativa de Spark (UDF)
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")

# Definimos las variables que el modelo espera exactamente en orden
variables_seleccionadas = [
    "monto_prom_6m", "frec_accessories_prom_6m", "recencia_dias",
    "yearly_income", "frec_clothing_prom_6m", "children_at_home",
    "total_children", "number_cars", "frec_bikes_prom_2m"
]


# Generamos el Score/Predicción usando la UDF de MLflow
df_predictions = customer_360_final.withColumn(
    "prediction", 
    predict_udf(*[F.col(c) for c in variables_seleccionadas])
)

# COMMAND ----------

df_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("catalog_analytics.sales_customers_seguimiento")