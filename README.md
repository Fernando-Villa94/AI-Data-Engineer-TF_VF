# **Customer Repurchase Prediction — IA DATA ENGINEER**



Pipeline de ingeniería de datos y Machine Learning para analizar el comportamiento de compra de los clientes y predecir su propensión a realizar una nueva compra durante los siguientes seis meses.



El proyecto implementa una arquitectura por capas en Databricks, con transformaciones SQL gestionadas mediante dbt, controles de calidad, generación de variables históricas y seguimiento de experimentos mediante MLflow.



#### **1. Objetivo del proyecto**



Desarrollar un pipeline de extremo a extremo que permita transformar datos de ventas, productos y clientes en información analítica lista para entrenar y ejecutar un modelo predictivo.



Los principales objetivos son:



* Integrar y procesar diferentes formatos de datos.
* Implementar una arquitectura de datos por capas.
* Aplicar reglas de limpieza, transformación y calidad.
* Construir variables históricas de comportamiento de compra.
* Predecir qué clientes tienen mayor propensión a volver a comprar en los siguientes seis meses.
* Registrar experimentos y gestionar los artefactos del modelo.
* Generar información para analizar ventas, clientes activos y oportunidades de recompra.





#### **2. Arquitectura de la solución**



La solución se desarrolla principalmente en Databricks Free Edition, utilizando tablas Delta para almacenar los datos procesados.



Fuentes de datos

&#x20; ├── sales.parquet

&#x20; ├── products.json

&#x20; └── customers.csv

&#x20;         │

&#x20;         ▼

&#x20;     INGESTA

&#x20;         │

&#x20;         ▼

&#x20; RAW — catalog\_raw

&#x20; ├── sales

&#x20; ├── products

&#x20; └── customers

&#x20;         │

&#x20;         ▼

&#x20; STAGING — catalog\_staging

&#x20; ├── stg\_sales

&#x20; ├── stg\_products

&#x20; └── stg\_customers

&#x20;         │

&#x20;         ▼

&#x20; INTERMEDIATE — catalog\_intermediate

&#x20; └── int\_customer\_monthly\_aggregates

&#x20;         │

&#x20;         ▼

&#x20; MARTS — catalog\_marts

&#x20; ├── dim\_customers

&#x20; ├── dim\_products

&#x20; ├── fct\_sales

&#x20; ├── customer\_360

&#x20; └── customer\_360\_historico

&#x20;         │

&#x20;         ▼

&#x20; ML / INFERENCIA

&#x20; ├── LightGBM

&#x20; └── MLflow

&#x20;         │

&#x20;         ▼

&#x20; ANALYTICS — catalog\_analytics

&#x20; └── sales\_customers\_seguimiento

&#x20;         │

&#x20;         ▼

&#x20; CONSUMO ANALÍTICO

&#x20; └── Power BI (visualización propuesta)



La arquitectura separa la ingesta, la preparación de datos, las transformaciones analíticas y la inferencia, facilitando el mantenimiento, la trazabilidad y la evolución de cada componente.



#### **3. Tecnologías utilizadas**



|Tecnología<br />|Uso<br />|
|-|-|
|Databricks Free Edition<br />Apache Spark / PySpark<br />Delta Lake<br />SQL<br />dbt<br />Python<br />LightGBM<br />MLflow<br />GitHub<br />Power BI<br />|Entorno principal de procesamiento y ejecución del pipeline.<br />Lectura, procesamiento y operaciones sobre datos distribuidos.<br />Almacenamiento de tablas y gestión de operaciones sobre datos.<br />Transformaciones, agregaciones y construcción de modelos analíticos.<br />Modularización de modelos SQL, documentación y pruebas de calidad.<br />Preparación de datos y desarrollo del modelo predictivo.<br />Algoritmo de Machine Learning para clasificación.<br />Seguimiento de experimentos y registro de artefactos del modelo.<br />Versionamiento del código y documentación.<br />Herramienta propuesta para visualizar los resultados del proyecto.<br />|



Nota: Power BI corresponde a la alternativa de consumo analítico planteada para el proyecto; su conexión y publicación dependen de la configuración disponible.



#### **4. Fuentes de datos**



Los archivos de entrada contienen información de ventas, productos y clientes.



|Archivo<br />|Tamaño<br />|Registros aproximados<br />|
|-|-|-|
|sales.parquet<br />products.json<br />customers.csv<br />|748 KB<br />83 KB<br />2 278 KB<br />|60 000<br />397<br />17 939<br />|
|Total<br />|3 109 KB<br />|78 336<br />|



Los datos se preparan para su carga en Databricks y se organizan en tablas que alimentan las diferentes etapas del pipeline.



#### **5. Capas de datos**



**RAW — catalog\_raw**



Contiene las tablas iniciales sales, products y customers. Constituye el punto de entrada de los datos al entorno de procesamiento.



**STAGING — catalog\_staging**



Contiene los modelos stg\_sales, stg\_products y stg\_customers. Se encarga de la conversión de tipos de datos, normalización básica y aplicación de reglas iniciales de calidad.



**INTERMEDIATE — catalog\_intermediate**



El modelo int\_customer\_monthly\_aggregates consolida las compras por cliente y periodo mensual, incluyendo montos de ventas y frecuencias por categoría de producto.



**MARTS — catalog\_marts**



Contiene las dimensiones de clientes y productos, la tabla de hechos de ventas y los modelos analíticos para el Machine Learning.



El modelo customer\_360 genera las variables para un periodo de análisis determinado. customer\_360\_historico conserva las observaciones históricas por cliente y periodo.



**ANALYTICS — catalog\_analytics**



La tabla sales\_customers\_seguimiento combina las variables históricas, la etiqueta de recompra cuando es observable y la predicción generada por el modelo.



Esta tabla está orientada al seguimiento de los resultados y al análisis de negocio.



#### **6. Modelo predictivo**



El proyecto utiliza LightGBM para estimar la propensión de recompra de los clientes.



Las variables de entrada incluyen:

* 
* Monto promedio de compras durante seis meses.
* Frecuencia de compra de accesorios durante seis meses.
* Recencia de compra.
* Ingresos anuales del cliente.
* Frecuencia de compra de ropa durante seis meses.
* Cantidad de hijos en el hogar y cantidad total de hijos.
* Número de automóviles.
* Frecuencia de compra de bicicletas durante dos meses.



La variable objetivo representa si el cliente registra una nueva compra durante los seis meses posteriores al periodo de análisis, siempre que exista información suficiente para observar esa ventana temporal.



La salida de inferencia incluye:



* customer\_id: identificador del cliente.
* periodo\_analisis: periodo de evaluación.
* Variables predictoras utilizadas por el modelo.
* target: resultado observado cuando la ventana futura es evaluable.
* prediction: puntuación numérica generada por el modelo.



La puntuación prediction debe interpretarse de acuerdo con la configuración del modelo y el procedimiento utilizado para convertirla en una clasificación. Para identificar clientes potenciales a recompra, se utiliza la regla definida por el proyecto para clasificar las predicciones.



#### **7. Calidad y gobierno de datos**



Se incorporan pruebas automatizadas mediante dbt en las capas STAGING, INTERMEDIATE y MARTS.Adicional se tienen validaciones para la capa RAW y Analytics previo a la ingesta de información. 



Los principales controles son:



* Completitud: identificación de valores nulos en campos críticos.
* Unicidad: validación de identificadores de ventas y combinaciones cliente-periodo.
* Integridad referencial: verificación de las relaciones entre ventas, clientes y productos.
* No negatividad: control de montos, frecuencias y variables numéricas seleccionadas.
* Trazabilidad: separación de modelos por capa y versionamiento del código.



La arquitectura, las reglas de calidad y las decisiones tecnológicas se documentan en los archivos architecture, data\_dictionary, governance y decisions del repositorio.



#### **8. Estructura del proyecto**



La siguiente estructura representa la organización lógica recomendada para el repositorio. Los nombres pueden ajustarse a la estructura definitiva implementada.



.

├── README.md

├── architecture/

│   └── architecture.png

├── ingestion/

├── processing/

├── dbt/

│   ├── models/

│   │   ├── staging/

│   │   ├── intermediate/

│   │   └── marts/

│   └── dbt\_project.yml

├── ml/

├── docs/

│   ├── data\_dictionary.md

│   ├── governance.md

│   └── decisions.md





Los directorios corresponden a una organización conceptual del proyecto. Deben actualizarse para reflejar exactamente las rutas y archivos existentes en el repositorio final.



#### **9. Ejecución del proyecto**



La ejecución sigue el orden lógico de dependencias:



* Preparar los archivos de entrada y cargarlos en las tablas RAW.
* Ejecutar los modelos de STAGING.
* Ejecutar las agregaciones de INTERMEDIATE.
* Construir las dimensiones, la tabla de hechos y los modelos MARTS.
* Generar o actualizar el histórico de variables por cliente y periodo.
* Cargar el modelo registrado en MLflow y ejecutar la inferencia.
* Guardar los resultados en catalog\_analytics.sales\_customers\_seguimiento.
* Conectar los resultados a la herramienta de visualización para el análisis de negocio.



Las transformaciones SQL se desarrollan y validan con dbt. Para la ejecución automatizada dentro de Databricks, las tareas pueden coordinarse mediante Databricks Jobs, según la configuración del entorno.



**Requisitos generales:**



* Acceso a un workspace de Databricks con los recursos necesarios.
* Archivos de entrada disponibles en la ubicación configurada.
* Código y dependencias del proyecto instalados o accesibles.
* Modelos dbt y pruebas correctamente configurados.
* Artefacto del modelo disponible en MLflow.
* Permisos para leer las tablas de entrada y escribir las tablas de salida.



Los nombres de catálogos, esquemas, ubicaciones de archivos y referencias al modelo deben coincidir con los configurados en el entorno de ejecución.



#### **10. Visualización y análisis de negocio**



La capa de consumo está orientada a responder preguntas sobre el comportamiento comercial y la propensión de recompra.



Los principales indicadores propuestos son:



* Evolución mensual de las ventas.
* Evolución de clientes activos por mes.
* Número de clientes clasificados como potenciales a recompra en el último periodo disponible.
* Monto histórico de compras de esos clientes durante los seis meses anteriores.
* Comparación de ventas por categoría de producto.
* Ranking de países según el monto de ventas asociado a sus clientes.



Para estos indicadores se debe definir con claridad el periodo analizado, el criterio de cliente activo y la diferencia entre ventas históricas observadas y compras futuras estimadas.



#### **11. Limitaciones y evolución futura**



La solución está diseñada como un proyecto académico y de demostración técnica, priorizando simplicidad, reproducibilidad y bajo costo.



Entre las posibles mejoras se encuentran:



* Implementar CI/CD para ejecutar validaciones antes de integrar cambios.
* Automatizar y monitorear las ejecuciones mediante Jobs.
* Incorporar monitoreo de calidad de datos y rendimiento del modelo.
* Versionar de manera explícita los datos de entrenamiento y los artefactos.
* Exponer predicciones mediante FastAPI si se requiere integración con aplicaciones externas.
* Evaluar servicios administrados y un entorno productivo con garantías operativas si aumentan los requerimientos.



#### **12. Documentación complementaria**



architecture/architecture.png: representación visual de la arquitectura.

docs/data\_dictionary.md: definición de capas, tablas y campos.

docs/governance.md: reglas de calidad y pruebas implementadas.

docs/decisions.md: decisiones arquitectónicas, alternativas, trade-offs y criterios de evolución.

