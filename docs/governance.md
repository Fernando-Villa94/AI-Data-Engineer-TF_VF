# **Data Governance y Calidad de Datos**





#### **1. Objetivo**



Establecer las reglas de calidad, controles de integridad y criterios de validación aplicados al pipeline de datos y Machine Learning. El objetivo es asegurar que los datos utilizados en las transformaciones, la construcción de variables y la inferencia cumplan condiciones mínimas de completitud, unicidad, integridad referencial y consistencia.

#### 

#### **2. Estrategia de validación por capa**



|Capa<br />|Estrategia de control<br />|
|-|-|
|RAW (catalog\_raw)<br />|Se valida el tipo de dato a ingestar de cada columna según la inferencia de esquema de los datos actuales (si existen), para evitar inconsistencias en las siguientes capas. Además se valida no nulidad de datos (debe haber registros en cada archivo a ingestar). <br />|
|STAGING (catalog\_staging)<br />|Valida completitud de identificadores, unicidad de ventas e integridad referencial entre ventas, clientes y productos.<br />|
|INTERMEDIATE (catalog\_intermediate)<br />|Valida la unicidad de la combinación cliente-periodo, la completitud de los principales indicadores mensuales y que los montos y frecuencias no sean negativos.<br />|
|MARTS (catalog\_marts)<br />|Valida claves primarias, integridad referencial de ventas, completitud de métricas y ausencia de valores negativos en las variables utilizadas por el modelo.<br />|
|ANALYTICS / INFERENCIA (catalog\_analytics)<br />|Cuenta con prueba de no nulidad de datos, no nulidad en la columna de predicción y target, así como pruebas de unicidad en llaves (se eliminan los duplicados y se almacena temporalmente en una vista).<br />|







#### **3. Reglas de calidad implementadas**



**3.1. Staging**



* Tipo de dato: los campos deben contener el tipo de dato esperado antes de ingestarse.
* Completitud: se espera que los archivos tengan datos. 





**3.2. Staging**



* Completitud: product\_id, customer\_id y sale\_id no pueden ser nulos.
* Unicidad: sale\_id debe identificar de manera única cada registro de ventas.
* Integridad referencial: cada customer\_id de ventas debe existir en stg\_customers y cada product\_id en stg\_products.



**3.3. Intermediate**



Modelo evaluado: int\_customer\_monthly\_aggregates.



* Unicidad: combinación de customer\_id y codmes.
* Completitud: customer\_id, codmes, monto\_mes y las frecuencias mensuales por categoría no pueden ser nulos.
* No negatividad: frec\_bikes\_mes, frec\_clothing\_mes y frec\_accessories\_mes deben ser mayores o iguales a cero.



**3.4. Marts**



* Dimensiones: dim\_products.product\_id y dim\_customers.customer\_id deben ser únicos y no nulos.
* Ventas: fct\_sales.sale\_id debe ser único y no nulo.
* Integridad referencial: los clientes y productos de fct\_sales deben existir en sus respectivas dimensiones.
* Completitud de ventas: quantity y sales\_total no pueden ser nulos.
* Completitud de variables: las variables seleccionadas de customer\_360 deben ser no nulas.
* No negatividad: las métricas de recencia, montos, frecuencias e indicadores sociodemográficos numéricos seleccionados deben ser mayores o iguales a cero.
* Precio: dim\_products.price no puede ser nulo.



**3.5. Analytics**



* Completitud: la cantidad de registros a ingestar no debe ser nula.
* No nulidad: los campos target y prediction no deben tener nulos.
* Unicidad: combinación 'customer\_id' y 'periodo\_analisis.





#### **4. Herramientas y ejecución**



Las pruebas se implementan mediante dbt, utilizando data\_tests y pruebas estándar (not\_null, unique, relationships), además de la prueba personalizada es\_mayor\_o\_igual\_a para validar valores mínimos.



Las pruebas se ejecutan sobre los modelos configurados en cada capa. Los resultados permiten identificar registros incompletos, claves duplicadas, referencias inexistentes y valores fuera de las reglas definidas.



#### **5. Limitaciones y mejoras propuestas**



* Añadir controles de rangos, consistencia de fechas y reglas de negocio para las variables principales.
* Registrar y monitorear los resultados de las pruebas para facilitar la trazabilidad de errores y la supervisión del pipeline.

