# Databricks notebook source
# MAGIC %md
# MAGIC 1. GENERACIÓN DE TARGET

# COMMAND ----------

from pyspark.sql import functions as F

customer_360 = spark.table("workspace.catalog_marts.customer_360_historico")
monthly_sales = spark.table("workspace.catalog_intermediate.int_customer_monthly_aggregates")

customer_360 = (
    customer_360
    .withColumn(
        "periodo_fecha",
        F.to_date(
            F.concat(
                F.col("periodo_analisis").cast("string"),
                F.lit("01")
            ),
            "yyyyMMdd"
        )
    )
)

monthly_sales = (
    monthly_sales
    .withColumn(
        "codmes_fecha",
        F.to_date(
            F.concat(
                F.col("codmes").cast("string"),
                F.lit("01")
            ),
            "yyyyMMdd"
        )
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
            # Mismo cliente
            (F.col("c.customer_id") == F.col("s.customer_id"))

            &

            # Solamente meses posteriores al periodo de análisis
            (F.col("s.codmes_fecha") > F.col("c.periodo_fecha"))

            &

            # Hasta seis meses posteriores
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
        .alias("target")
    )
)

customer_360_final = (
    customer_360_valid
    .drop("periodo_fecha")
    .join(
        target,
        on=[
            "customer_id",
            "periodo_analisis"
        ],
        how="left"
    )
)

customer_360_final.write.mode("overwrite").format("parquet").save("/Volumes/Workspace/default/seguimiento_tf_ai_de/")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. ENTRENAMIENTO

# COMMAND ----------

df_tablon_modelo=spark.read.parquet("/Volumes/Workspace/default/seguimiento_tf_ai_de")

# COMMAND ----------

# MAGIC %pip install imblearn

# COMMAND ----------

df_train=df_tablon_modelo.filter(F.col("periodo_analisis").isin(202507,202508,202509,202510,202511,202512,202601))
df_test=df_tablon_modelo.filter(F.col("periodo_analisis").isin(202602))
decimal_cols = [
    "monto_prom_6m",
    "frec_accessories_prom_6m",
    "frec_clothing_prom_6m",
    "frec_bikes_prom_2m"
]
select_exprs = [
    F.col(c).cast("double").alias(c) if c in decimal_cols else F.col(c)
    for c in df_train.columns
]

# Aplicamos la selección a ambos DataFrames
df_train = df_train.select(*select_exprs)
df_test = df_test.select(*select_exprs)

train_pd = df_train.toPandas()
test_pd = df_test.toPandas()

variables_seleccionadas = [
    "monto_prom_6m",
    "frec_accessories_prom_6m",
    "recencia_dias",
    "yearly_income",
    "frec_clothing_prom_6m",
    "children_at_home",
    "total_children",
    "number_cars",
    "frec_bikes_prom_2m"
]

target_col = "target"

X_train = train_pd[variables_seleccionadas]
y_train = train_pd[target_col]

X_test = test_pd[variables_seleccionadas]
y_test = test_pd[target_col]




from imblearn.under_sampling import RandomUnderSampler

undersampler = RandomUnderSampler(
    random_state=42
)

X_train_under, y_train_under = undersampler.fit_resample(
    X_train,
    y_train
)



# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

from lightgbm import LGBMClassifier

model = LGBMClassifier(
    colsample_bytree=1,
    max_depth=7,
    num_leaves=31,
    subsample=0.7,
    random_state=42,
    n_estimators=150,
    objective="binary"
)

model.fit(
    X_train_under,
    y_train_under
)

# COMMAND ----------

# MAGIC %pip install seaborn
# MAGIC

# COMMAND ----------

from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# =============================================================================
# 1. GENERAR PREDICCIONES
# =============================================================================

# Predicciones sobre TRAIN
y_pred_train = model.predict(X_train)

# Predicciones sobre TEST OOT
y_pred_test = model.predict(X_test)


# =============================================================================
# 2. MATRICES DE CONFUSIÓN NORMALIZADAS POR FILA
# =============================================================================

cm_train = confusion_matrix(
    y_train,
    y_pred_train,
    normalize="true"
)

cm_test = confusion_matrix(
    y_test,
    y_pred_test,
    normalize="true"
)


# =============================================================================
# 3. GRÁFICO
# =============================================================================

fig, axes = plt.subplots(
    1,
    2,
    figsize=(14, 5.5),
    sharey=True
)

etiquetas = [
    "No Compra (0)",
    "Compra (1)"
]


# -----------------------------------------------------------------------------
# TRAIN
# -----------------------------------------------------------------------------

sns.heatmap(
    cm_train,
    annot=True,
    fmt=".2%",
    cmap="Blues",
    xticklabels=etiquetas,
    yticklabels=etiquetas,
    ax=axes[0],
    cbar=False,
    annot_kws={
        "size": 12,
        "weight": "bold"
    }
)

axes[0].set_title(
    "Matriz de Confusión - TRAIN (Pasado)",
    fontsize=13,
    pad=10,
    weight="bold"
)

axes[0].set_xlabel(
    "Predicción del Modelo",
    fontsize=11
)

axes[0].set_ylabel(
    "Clase Real (True Label)",
    fontsize=11
)


# -----------------------------------------------------------------------------
# TEST OOT
# -----------------------------------------------------------------------------

sns.heatmap(
    cm_test,
    annot=True,
    fmt=".2%",
    cmap="Oranges",
    xticklabels=etiquetas,
    yticklabels=etiquetas,
    ax=axes[1],
    cbar=False,
    annot_kws={
        "size": 12,
        "weight": "bold"
    }
)

axes[1].set_title(
    "Matriz de Confusión - TEST OOT (Futuro)",
    fontsize=13,
    pad=10,
    weight="bold"
)

axes[1].set_xlabel(
    "Predicción del Modelo",
    fontsize=11
)

plt.tight_layout()
plt.show()

# COMMAND ----------

from sklearn.metrics import classification_report


y_pred_train =model.predict(X_train)
y_pred_test = model.predict(X_test)

# 2. Imprimir los reportes de clasificación detallados
print("=====================================================================")
print(" REPORT DE CLASIFICACIÓN: DATASET DE TRAIN (PASADO)")
print("=====================================================================")
print(classification_report(y_train, y_pred_train, target_names=["No Compra (0)", "Compra (1)"], digits=4))

print("\n=====================================================================")
print(" REPORTE DE CLASIFICACIÓN: DATASET DE TEST OOT (FUTURO)")
print("=====================================================================")
print(classification_report(y_test, y_pred_test, target_names=["No Compra (0)", "Compra (1)"], digits=4))

# COMMAND ----------

# MAGIC %md
# MAGIC mlflow

# COMMAND ----------

import mlflow
import mlflow.lightgbm
from lightgbm import LGBMClassifier
from sklearn.metrics import classification_report

# 2. Activar Autologging (MLflow capturará automáticamente los parámetros del LGBMClassifier)
mlflow.lightgbm.autolog()

# 3. Iniciar el contexto de ejecución de MLflow
with mlflow.start_run(run_name="LGBM_UnderSampling_PrecisionFix") as run:
    
    # Instanciamos el modelo con tus hiperparámetros
    model = LGBMClassifier(
        colsample_bytree=1,
        max_depth=7,
        num_leaves=31,
        subsample=0.7,
        random_state=42,
        n_estimators=150,
        objective="binary"
    )

    # El fit registrará automáticamente los parámetros e importancia de variables
    model.fit(
        X_train_under,
        y_train_under
    )

    # 4. Calcular predicciones para las métricas de negocio
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # 5. Generar y registrar diccionarios de métricas clave (Enfocados en la clase 1)
    report_train = classification_report(y_train, y_pred_train, output_dict=True)
    report_test = classification_report(y_test, y_pred_test, output_dict=True)

    # Registramos explícitamente el Recall y F1 de Test que tanto nos importan
    mlflow.log_metric("train_recall_clase_1", report_train["1"]["recall"])
    mlflow.log_metric("train_f1_clase_1", report_train["1"]["f1-score"])
    
    mlflow.log_metric("test_recall_clase_1", report_test["1"]["recall"])
    mlflow.log_metric("test_f1_clase_1", report_test["1"]["f1-score"])
    mlflow.log_metric("test_accuracy", report_test["accuracy"])

    # 6. Guardar los reportes en formato texto como artefactos legibles
    reporte_txt_train = classification_report(y_train, y_pred_train)
    reporte_txt_test = classification_report(y_test, y_pred_test)

    with open("classification_report_train.txt", "w") as f:
        f.write(reporte_txt_train)
    with open("classification_report_test.txt", "w") as f:
        f.write(reporte_txt_test)

    mlflow.log_artifact("classification_report_train.txt")
    mlflow.log_artifact("classification_report_test.txt")

    print(f"Ejecución registrada exitosamente. Run ID: {run.info.run_id}")