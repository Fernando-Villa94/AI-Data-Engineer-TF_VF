# **Architecture Decision Records (ADR)**





#### **1. Objetivo y criterios de decisión**



Este documento registra las decisiones arquitectónicas adoptadas para implementar un pipeline de datos y Machine Learning orientado a predecir la recompra de clientes durante los siguientes seis meses.



Los principales criterios utilizados son:



* Costo: priorizar herramientas gratuitas o con versiones gratuitas suficientes para el desarrollo académico.
* Capacidad: seleccionar tecnologías adecuadas para el volumen actual de datos.
* Reproducibilidad: permitir la ejecución repetible del pipeline y el seguimiento de experimentos.
* Calidad y gobernanza: incorporar transformaciones controladas, pruebas automatizadas y trazabilidad.
* Mantenibilidad: organizar el código para facilitar su versionamiento, revisión y futura migración a producción.
* Experiencia técnica: aprovechar conocimientos previos de Databricks, Python, SQL, dbt y herramientas de Machine Learning.



La arquitectura prioriza una solución funcional de extremo a extremo, manteniendo separadas la preparación de datos, la generación de variables, el entrenamiento o registro del modelo y el consumo de resultados.



#### **2. ADR-01: Ingesta de datos**



**Decisión**



Utilizar archivos locales en formatos CSV, JSON y Parquet como fuentes de entrada, cargándolos en Databricks para su procesamiento posterior.



**Alternativas consideradas**



* Azure Data Factory: automatización de cargas desde diferentes sistemas y servicios.
* Google Cloud Dataflow: procesamiento e ingesta en el ecosistema GCP.
* Apache Airflow: orquestación de procesos de extracción y carga.
* Lectura directa de los archivos mediante scripts Python ejecutados fuera de Databricks.



**Motivo**



Los archivos disponibles tienen un volumen reducido:



|Archivo<br />|Tamaño<br />|Registros aproximados<br />|
|-|-|-|
|sales.parquet<br />products.json<br />customers.csv<br />|748 KB<br />83 KB<br />2 278 KB<br />|60 000<br />397<br />17 939<br />|
|Total<br />|3 109 KB<br />|78 336<br />|







El volumen total es de aproximadamente 3 MB, por lo que no se justifica incorporar inicialmente una plataforma de ingesta distribuida adicional. Databricks permite centralizar la lectura y el procesamiento de los archivos, simplificando la arquitectura.



**Trade-off**



Se gana: simplicidad, bajo costo, menor cantidad de componentes y facilidad de reproducción.

Se sacrifica: automatización de cargas desde sistemas externos, gestión avanzada de conectores y mecanismos especializados para ingestas incrementales.



**Cambio futuro**



Se incorporaría una herramienta de ingesta como Azure Data Factory, Airflow u otra solución equivalente si el proyecto necesitara conectarse periódicamente a bases de datos productivas, APIs externas o fuentes con cargas incrementales y dependencias operativas.



#### **3. ADR-02: Capa RAW**



**Decisión**



Almacenar los datos de entrada en tablas Delta dentro del esquema catalog\_raw, conservando las entidades principales: sales, products y customers.



**Alternativas consideradas**



* Almacenamiento de archivos directamente en un data lake sin materializar tablas.
* Uso de una base de datos relacional como staging inicial.
* Almacenamiento en una plataforma cloud externa, como Azure Data Lake Storage o Google Cloud Storage.



**Motivo**



Delta permite trabajar con tablas estructuradas directamente desde Databricks y facilita su utilización por las capas posteriores. Se mantiene una separación lógica entre la información inicial y las transformaciones de negocio.



La capa RAW conserva los datos tal como fueron preparados para la carga. 



**Trade-off**



Se gana: acceso centralizado, estructura tabular, integración con Spark y separación de responsabilidades entre capas.

Se sacrifica: independencia completa respecto de la plataforma Databricks y preservación exacta del origen cuando existen transformaciones previas a la carga.



**Cambio futuro**



En producción, se incorporaría almacenamiento persistente independiente, control de versiones de los archivos de origen, metadatos de ingesta y mecanismos de recuperación. También se conservaría una copia original inmutable cuando la trazabilidad lo requiera.



#### **4. ADR-03: Procesamiento y limpieza — STAGING**



**Decisión**



Utilizar Databricks para ejecutar las transformaciones iniciales y dbt para definir, organizar y validar los modelos SQL de la capa catalog\_staging.



**Alternativas consideradas**



* Transformaciones implementadas exclusivamente mediante notebooks PySpark.
* Azure Data Factory con transformaciones adicionales.
* dbt Core ejecutado localmente.
* Procesamiento mediante servicios de transformación de Google Cloud.



**Motivo**



Databricks ofrece el entorno de ejecución, mientras que dbt permite organizar las transformaciones SQL como modelos versionables y aplicar pruebas de calidad de manera declarativa.



Se utilizó dbt Cloud por su facilidad de conexión con Databricks y la rapidez para desarrollar y probar los modelos. Su periodo gratuito de evaluación fue suficiente para el desarrollo del proyecto. Posteriormente, el código se conserva en GitHub y puede ejecutarse desde el repositorio correspondiente en Databricks.



La capa STAGING realiza conversiones de tipos de datos, normalización básica y tratamiento de determinados valores inválidos. También incorpora pruebas de completitud, unicidad e integridad referencial.



**Trade-off**



Se gana: código SQL modular, pruebas automatizadas, documentación y facilidad para revisar cambios antes de integrarlos mediante un pull request.

Se sacrifica: una herramienta adicional que debe configurarse y mantenerse, además de la dependencia temporal de dbt Cloud durante el desarrollo inicial.



**Cambio futuro**



Ejecutar pruebas en CI/CD o establecer despliegues controlados entre ambientes, por ejemplo con GitHub actions.



#### **5. ADR-04: Curated — dimensiones, hechos y agregaciones**



**Decisión**



Implementar las capas catalog\_intermediate y catalog\_marts en Databricks, utilizando modelos SQL gestionados mediante dbt.



La capa INTERMEDIATE genera agregaciones mensuales por cliente y categoría. La capa MARTS organiza las dimensiones de clientes y productos, la tabla de hechos de ventas y los modelos analíticos destinados al consumo posterior.



**Alternativas consideradas**



* Azure Synapse Analytics o Microsoft Fabric como plataforma analítica.
* BigQuery como data warehouse administrado.
* Una base de datos relacional tradicional.
* Transformaciones consolidadas en un único notebook PySpark.



**Motivo**



Mantener las transformaciones dentro de Databricks evita introducir un motor adicional y permite reutilizar las tablas Delta creadas en las capas anteriores.



La separación entre agregaciones intermedias y modelos de negocio mejora la organización del código. dbt facilita la trazabilidad entre modelos y permite validar claves, relaciones y reglas de calidad.



**Trade-off**



Se gana: arquitectura por capas, reutilización de transformaciones, integración con el entorno de procesamiento y menor complejidad operativa.

Se sacrifica: independencia respecto de Databricks y las capacidades especializadas de un data warehouse externo para escenarios empresariales de mayor escala.



**Cambio futuro**



Se evaluaría un data warehouse dedicado si aumentaran significativamente la concurrencia de consultas, los requerimientos de disponibilidad, las necesidades de aislamiento entre cargas o los volúmenes de datos. La decisión dependería de mediciones de rendimiento y costos, no únicamente del crecimiento del volumen.



#### **6. ADR-05: Features — construcción del conjunto de variables**



**Decisión**



Construir customer\_360 y su versión histórica mediante SQL y dbt, utilizando las agregaciones mensuales de INTERMEDIATE y los atributos de clientes de STAGING.



Las variables se calculan en ventanas temporales definidas para el modelo, incluyendo montos promedio, frecuencias de compra, recencia y atributos sociodemográficos.



**Alternativas consideradas**



* Feature Store administrado.
* Cálculo de variables directamente en Python durante la inferencia.
* Generación de variables mediante notebooks PySpark independientes de dbt.
* Uso de una plataforma especializada de feature engineering.



**Motivo**



El número de variables y el volumen actual de datos no justifican incorporar una plataforma de Feature Store. Los modelos SQL permiten expresar las reglas de cálculo de manera transparente, versionable y verificable.



La construcción histórica permite generar observaciones por cliente y periodo, facilitando la preparación de datos para entrenamiento, evaluación y seguimiento de las predicciones.



**Trade-off**



Se gana: transparencia en las reglas de negocio, reproducibilidad de las variables, menor costo y separación entre ingeniería de datos y modelado.

Se sacrifica: capacidades avanzadas de sincronización entre entrenamiento e inferencia, administración centralizada de features y control automático de su reutilización entre múltiples modelos.



**Cambio futuro**



Se evaluaría un Feature Store si varios modelos compartieran las mismas variables, se necesitara servirlas en tiempo real o fuera necesario controlar rigurosamente la consistencia entre las variables utilizadas en entrenamiento e inferencia.



#### **7. ADR-06: Machine Learning y experimentación**



**Decisión**



Utilizar Python y LightGBM para el modelo predictivo, junto con MLflow para registrar experimentos y conservar los artefactos del modelo. La inferencia se ejecuta sobre las variables preparadas en Databricks y genera una predicción de propensión a recompra.



**Alternativas consideradas**



* Scikit-learn con registro manual de modelos y métricas.
* Azure Machine Learning para entrenamiento y administración del ciclo de vida del modelo.
* Vertex AI para experimentación y despliegue en GCP.
* Entrenamiento con LightGBM sin una herramienta de seguimiento de experimentos.



**Motivo**



LightGBM permite desarrollar un modelo de clasificación adecuado para datos tabulares. MLflow ofrece seguimiento de experimentos y registro de parámetros, métricas y artefactos, además de facilitar la recuperación del modelo utilizado.



Su integración con Databricks permite mantener los experimentos dentro del entorno del proyecto sin incorporar inicialmente una plataforma MLOps adicional.



La arquitectura busca que el modelo sea reproducible y que las predicciones puedan vincularse con las variables históricas utilizadas. La reproducibilidad completa requiere, además, versionar el código, registrar la configuración y preservar la versión de los datos de entrenamiento.



**Trade-off**



Se gana: trazabilidad experimental, registro de métricas, reutilización de artefactos y una ruta clara para comparar versiones del modelo.

Se sacrifica: capacidades avanzadas de despliegue, monitoreo y gobierno empresarial que podrían ofrecer plataformas MLOps administradas. El registro de experimentos, por sí solo, tampoco garantiza la reproducción exacta del entrenamiento.



**Cambio futuro**



Se incorporaría una plataforma MLOps más completa (como Azure ML) si el modelo pasara a producción y necesitara despliegues controlados, gestión formal de versiones, monitoreo de drift, alertas, evaluación periódica y procedimientos de rollback.



#### **8. ADR-07: Consumo y visualización de resultados**



**Decisión**



Utilizar Power BI Desktop como interfaz principal de consumo analítico, alimentado por las tablas de resultados generadas en catalog\_analytics.



**Alternativas consideradas**



* Power BI: dashboards interactivos orientados al análisis de negocio.
* FastAPI: exposición de predicciones y datos mediante endpoints REST.
* Dashboards desarrollados en Streamlit o Dash.
* Visualizaciones directamente dentro de Databricks.



**Motivo**



El objetivo principal del consumo es analizar el comportamiento de ventas y clientes, no solamente solicitar predicciones individuales. Power BI facilita construir visualizaciones interactivas, comparar periodos y explorar indicadores de negocio.



El dashboard permitirá analizar:



Evolución de ventas: monto de ventas por mes.

Clientes activos mensuales: cantidad de clientes con compras en cada periodo.

Clientes potenciales a recompra: clientes con prediction = 1 para el último periodo disponible.

Monto histórico de los clientes potenciales: compras acumuladas durante los seis meses anteriores.

Ventas por categoría: comparación entre bicicletas, ropa y accesorios.

Ranking geográfico: países con mayores ventas según el país asociado al cliente.



Para evitar interpretaciones incorrectas, el indicador de clientes potenciales debe corresponder al último periodo de inferencia disponible. Asimismo, el monto histórico de seis meses debe calcularse como una métrica de ventas observadas, no como una estimación de ventas futuras.



Power BI puede conectarse a Databricks mediante su conector correspondiente. La modalidad de conexión y publicación dependerá de los recursos y permisos disponibles en el entorno.



**Trade-off**



Se gana: mejor experiencia de análisis, filtros interactivos, visualización de indicadores de negocio y presentación clara de los resultados del modelo.

Se sacrifica: capacidad de exponer directamente el modelo como un servicio consumible por aplicaciones externas. Además, la conexión directa a Databricks y la publicación en Power BI Service pueden depender de la configuración y disponibilidad de los recursos correspondientes.



FastAPI, por su parte, permitiría servir predicciones mediante endpoints, pero no proporciona por sí solo un dashboard de negocio.



**Cambio futuro**



Se incorporaría FastAPI si una aplicación externa necesitara enviar datos de clientes y obtener predicciones bajo demanda. En un escenario productivo, la API debería incluir autenticación, validación de entradas, registro de solicitudes, control de errores y monitoreo.



Si el objetivo evolucionara hacia la operación automatizada del modelo, se complementaría el dashboard con la API, en lugar de reemplazar Power BI.



#### **9. ADR-08: Orquestación y ejecución del pipeline**



**Decisión**



Utilizar Databricks Jobs para coordinar la ejecución de los notebooks y tareas del pipeline, respetando el orden de dependencias entre ingesta, procesamiento, generación de variables e inferencia.



dbt se utiliza para desarrollar, documentar y probar los modelos SQL. La ejecución final de estos modelos se realiza desde el entorno definido para el pipeline.



**Alternativas consideradas**



* Apache Airflow para la orquestación de tareas.
* Azure Data Factory para coordinar servicios de datos.
* GitHub Actions para automatizar etapas de validación y ejecución asociadas al repositorio.
* Orquestación manual desde notebooks.



**Motivo**



Databricks Jobs permite centralizar la ejecución de tareas que ya se encuentran dentro de Databricks, evitando incorporar un orquestador independiente para un pipeline de alcance académico.



La combinación de GitHub para versionamiento, dbt para las transformaciones y pruebas, y Jobs para coordinar la ejecución permite separar el desarrollo del código de su ejecución operativa.



**Trade-off**



Se gana: menor complejidad arquitectónica, integración nativa con el procesamiento y una secuencia de ejecución centralizada.

Se sacrifica: capacidades de orquestación transversal entre múltiples plataformas y servicios externos que pueden ser importantes en arquitecturas empresariales complejas.



**Cambio futuro**



Se evaluaría Airflow, Azure Data Factory u otro orquestador cuando el pipeline dependiera de múltiples plataformas, tuviera numerosas dependencias externas o requiriera capacidades avanzadas de planificación, recuperación y monitoreo.



#### **10. ADR-09: Versionamiento y flujo de desarrollo**



**Decisión**



Utilizar GitHub como repositorio del código, las definiciones de dbt y la documentación técnica. Los cambios se organizan mediante ramas y pull requests antes de integrarse a la rama principal.



**Alternativas consideradas**



* Desarrollo exclusivamente dentro de Databricks.
* Almacenamiento de scripts en carpetas locales.
* Azure DevOps Repos.
* GitLab.



**Motivo**



GitHub facilita el control de versiones, la revisión de cambios y la conservación de la estructura del proyecto. El repositorio permite clonar el código en Databricks y mantener las transformaciones separadas de los datos y artefactos generados.



El uso de dbt Cloud durante el desarrollo permite validar modelos y pruebas antes de integrar los cambios al repositorio definitivo.



**Trade-off**



Se gana: historial de cambios, colaboración, revisión de código y mejor reproducibilidad del desarrollo.

Se sacrifica: configuración inicial del flujo Git y necesidad de mantener sincronizados el repositorio, el entorno de desarrollo y las versiones de los artefactos.



**Cambio futuro**



Se implementaría CI/CD formal con ejecución automática de pruebas, validación de código, control de ambientes y despliegues aprobados si el proyecto se integrara en un entorno productivo.

