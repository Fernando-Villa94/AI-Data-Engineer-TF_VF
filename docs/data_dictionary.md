**Data Dictionary — Customer Repurchase Prediction**



##### **1. Propósito del documento**



Este documento describe la estructura, el significado y las reglas de transformación de los datos utilizados en el proyecto de predicción de recompra de clientes.



La fuente de información es AdventureWorksDW2019, una base de datos dimensional que contiene información de clientes, productos y transacciones comerciales.





##### **2. Capa Raw — catalog\_raw**



***2.1. Descripción general***



La capa Raw contiene las tres tablas Delta que reciben los datos de los archivos generados a partir de AdventureWorksDW2019.



En esta capa se conservan los nombres y valores de los campos extraídos, sin aplicar todavía las conversiones definitivas a sus tipos analíticos. Los campos se almacenan como STRING para simplificar la ingesta inicial y delegar la tipificación en la capa Staging.



Aunque se conserva la estructura de los datos extraídos, existen transformaciones realizadas durante la generación de los archivos. Estas incluyen la selección de campos, la asignación de nombres, la clasificación de clientes, los ajustes de precios y cantidades, y la modificación de determinadas fechas.



Por tanto, esta capa representa los datos de entrada del proyecto, pero no constituye una copia íntegra e inalterada de las tablas originales de AdventureWorksDW2019.



***2.2. Tabla customers***



Tabla física: catalog\_raw.customers



Fuente principal: customers.csv





Granularidad: un registro por cliente identificado mediante customer\_id, sujeto a la integridad de la fuente.



La extracción es lineal y se castea todo a string.



|Campo|Definición|
|-|-|
|customer\_id<br />signup\_date<br />city<br />state<br />country<br />segment<br />birth\_date<br />gender<br />marital\_status<br />yearly\_income<br />total\_children<br />children\_at\_home<br />education<br />occupation<br />house\_owner<br />number\_cars<br />|Identificador único del cliente.<br />Fecha de primera compra del cliente.<br />Ciudad de residencia del cliente.<br />Estado o provincia de residencia.<br />País o región geográfica.<br />Segmento del cliente según la presencia de hijos en el hogar.<br />Fecha de nacimiento del cliente.<br />Género registrado del cliente.<br />Estado civil registrado del cliente.<br />Ingreso anual registrado del cliente.<br />Número total de hijos del cliente.<br />Número de hijos que viven en el hogar.<br />Nivel educativo registrado.<br />Ocupación registrada.<br />Indicador de propiedad de vivienda.<br />Número de automóviles que posee el cliente.<br />|





***2.3. Tabla products***



Tabla física: catalog\_raw.products



Fuente principal: products.json



Granularidad: un registro por producto terminado seleccionado de la fuente.



La extracción es lineal y se castea todo a string.



|Campo|<br />Descripción<br />|
|-|-|
|product\_id<br />category<br />subcategory<br />brand<br />description<br />price|Identificador del producto.<br />Categoría comercial del producto.<br />Subcategoría comercial del producto.<br />Nombre del modelo asociado al producto.<br />Nombre descriptivo del producto.<br />Precio de referencia del producto.|



**2.4. Tabla sales**



Tabla física: catalog\_raw.sales



Fuente principal: sales.parquet



Granularidad: una línea de venta por combinación de pedido y producto, según el detalle de FactInternetSales.



La extracción es lineal y se castea todo a string.



|Campo|<br />Descripción<br />|
|-|-|
|sale\_id<br />customer\_id<br />product\_id<br />timestamp<br />quantity<br />unit\_price<br />sales\_amount|Identificador de la línea de venta.<br />Identificador del cliente que realizó la compra.<br />Identificador del producto vendido.<br />Fecha de la transacción.<br />Cantidad de unidades vendidas.<br />Precio unitario ajustado de la transacción.<br />Importe total de la línea de venta.|





##### **3. Capa Staging — catalog\_staging**



***3.1. Descripción general***



La capa Staging utiliza modelos dbt para convertir los datos de entrada en estructuras tipificadas y preparadas para el procesamiento analítico.



En esta capa se realizan las siguientes operaciones:



Conversión explícita de los campos a tipos de datos adecuados.

Normalización básica de valores y tratamiento de determinados nulos.

Aplicación de reglas iniciales de validez y consistencia.

Incorporación de una marca temporal de procesamiento mediante current\_timestamp().



La marca temporal fecha\_ingesta\_staging identifica el momento de procesamiento del registro por el modelo correspondiente. No equivale necesariamente a la fecha de llegada original del archivo ni garantiza por sí sola la detección de cambios entre ejecuciones.



***3.2. Tabla stg\_customers***



Tabla lógica: catalog\_staging.stg\_customers



Modelo dbt de origen: catalog\_raw.customers



Granularidad esperada: un registro por cliente.



|Campo	|Tipo de dato|Descripción / regla|
|-|-|-|
|customer\_id<br />signup\_date<br />city<br />state<br />country<br />segment<br />birth\_date<br />gender<br />marital\_status<br />yearly\_income<br />total\_children<br />children\_at\_home<br />education<br />occupation<br />house\_owner<br />number\_cars<br />fecha\_ingesta\_staging|INT<br />DATE<br />STRING<br />STRING<br />STRING<br />STRING<br />DATE<br />STRING<br />STRING<br />DECIMAL(15,2)<br />INT<br />INT<br />STRING<br />STRING<br />INT<br />INT<br />Tipo temporal devuelto por current\_timestamp()|<br />Identificador del cliente.<br />Fecha de primera compra, tipificada como fecha.<br />Ciudad de residencia.<br />Estado o provincia.<br />País o región geográfica.<br />Clasificación derivada de la presencia de hijos en el hogar.<br />Fecha de nacimiento.<br />Género normalizado durante la extracción.<br />Estado civil normalizado durante la extracción.<br />Ingreso anual con dos decimales.<br />Número total de hijos.<br />Número de hijos en el hogar.<br />Nivel educativo.<br />Ocupación.<br />Indicador de propiedad de vivienda.<br />Número de automóviles.<br />Momento de procesamiento del modelo Staging.<br />|







Reglas de transformación:



Los identificadores y contadores se convierten a enteros.

signup\_date y birth\_date se convierten a DATE.

yearly\_income se convierte a DECIMAL(15,2).

Los campos categóricos y geográficos se conservan como cadenas de texto.

Se añade fecha\_ingesta\_staging para registrar el momento de procesamiento.





***3.3. Tabla stg\_products***



Tabla lógica: catalog\_staging.stg\_products



Modelo dbt de origen: catalog\_raw.products



Granularidad esperada: un registro por producto.



|Campo|Tipo de dato|Descripción / regla|
|-|-|-|
|product\_id<br />category<br />subcategory<br />brand<br />description<br />Price<br /><br />fecha\_ingesta\_staging<br />|INT<br />STRING<br />STRING<br />STRING<br />STRING<br />DECIMAL(10,2)<br /><br />Tipo temporal devuelto por current\_timestamp()<br />|Identificador del producto.<br />Categoría del producto.<br />Subcategoría del producto.<br />Nombre del modelo asociado al producto.<br />Nombre descriptivo del producto.<br />Precio de referencia convertido a decimal. Si la conversión produce un nulo, se asigna 0.00.<br /><br />Momento de procesamiento del modelo Staging.<br />|









Reglas de transformación:



product\_id se convierte a entero.

Los campos descriptivos se conservan como cadenas de texto.

price se convierte a DECIMAL(10,2).

Se utiliza COALESCE para asignar 0.00 cuando el resultado de la conversión de price sea nulo.

Se incorpora la marca temporal fecha\_ingesta\_staging.



***3.4. Tabla stg\_sales***



Tabla lógica: catalog\_staging.stg\_sales



Modelo dbt de origen: catalog\_raw.sales



Granularidad esperada: un registro por orden de pedido.





|Campo|Tipo de dato|Descripción / regla|
|-|-|-|
|sale\_id<br />customer\_id<br />product\_id<br />timestamp<br />quantity<br />unit\_price<br />sales\_total<br />fecha\_ingesta\_staging<br />|STRING<br />INT<br />INT<br />DATE<br />INT<br />DOUBLE<br />DOUBLE<br />Tipo temporal devuelto por current\_timestamp()<br />|Identificador del pedido<br />Identificador del cliente.<br />Identificador del producto vendido.<br />Fecha de la transacción.<br />Cantidad vendida. Negativos se reemplazan por cero.<br />Precio unitario. Negativos y nulos se reemplazan por cero.<br />Cantidad \* PrecioUntario<br />Momento de procesamiento del modelo Staging.<br />|









Reglas de transformación:



Se tipifican los identificadores, la fecha, la cantidad y el precio unitario.

Los valores negativos de quantity se reemplazan por cero.

Los valores negativos de unit\_price se reemplazan por cero.

Los valores nulos de unit\_price se convierten a 0.00.

Se incorpora fecha\_ingesta\_staging.









##### **4. Capa Intermediate — catalog\_intermediate**



***4.1. Descripción general***



La capa Intermediate contiene modelos dbt que integran los datos preparados en Staging y generan estructuras analíticas derivadas.



A diferencia de Staging, cuyo objetivo principal es tipificar y aplicar reglas iniciales de calidad, Intermediate incorpora lógica de negocio y cálculos orientados al análisis del comportamiento de compra de los clientes.



***4.2. Tabla int\_customer\_monthly\_aggregates***



Tabla lógica: catalog\_intermediate.int\_customer\_monthly\_aggregates



Modelos dbt de origen:



stg\_sales

stg\_products



Granularidad: una fila por combinación de customer\_id y codmes que tenga registros de ventas.



Propósito: resumir el importe mensual de las ventas de cada cliente y generar indicadores de frecuencia de compra por categoría de producto.



***4.2.1. Lógica de transformación***



Paso 1. Lectura de las fuentes



Se consultan los modelos stg\_sales y stg\_products mediante las referencias de dbt (ref).



Paso 2. Integración de ventas y productos



Se realiza una unión izquierda entre ventas y productos utilizando product\_id.



La unión permite asociar cada transacción con la categoría del producto correspondiente. Si no se encuentra una categoría, se utiliza el valor unknown como sustituto.



Paso 3. Generación del periodo mensual



El campo codmes se construye a partir del año y mes de la transacción:



codmes = year(timestamp) \* 100 + month(timestamp)



Por ejemplo, una transacción de octubre de 2026 genera el código 202610.



Este campo facilita la agrupación cronológica por mes.



Paso 4. Normalización de la categoría



La categoría del producto se normaliza mediante las siguientes operaciones:



COALESCE: sustituye categorías nulas por unknown.

TRIM: elimina espacios al inicio y al final.

LOWER: convierte el texto a minúsculas.



La normalización permite comparar categorías de manera consistente.



Paso 5. Agregación mensual



Se agrupan las ventas por cliente y periodo mensual. Para cada grupo se calculan el importe mensual y las frecuencias por categoría.



***4.2.2. Diccionario de campos***







|Campo<br />|Tipo lógico esperado<br />|Descripción / regla<br />|
|-|-|-|
|customer\_id<br /><br />codmes<br />|INT<br /><br />INT<br />|Identificador del cliente procedente de stg\_sales.<br /><br />Periodo mensual con formato AAAAMM, derivado de la fecha de la transacción.<br />|
|<br />monto\_mes<br /><br /><br />frec\_bikes\_mes<br /><br /><br />frec\_clothing\_mes<br /><br /><br />frec\_accessories\_mes<br />|Numérico o float<br /><br /><br />BIGINT o tipo entero equivalente<br /><br /><br />BIGINT o tipo entero equivalente<br /><br /><br />BIGINT o tipo entero equivalente<br />|Suma de sales\_amount, cuyo origen es stg\_sales.sales\_total, para el cliente y mes correspondientes.<br /><br />Número de líneas de venta asociadas a productos de la categoría bikes durante el mes.<br /><br />Número de líneas de venta asociadas a productos de la categoría clothing durante el mes.<br /><br />Número de líneas de venta asociadas a productos de la categoría accessories durante el mes.<br />|







Los tipos numéricos definitivos de las columnas agregadas dependen del motor SQL y de la materialización del modelo dbt.



***4.2.3. Definición de las métricas***



monto\_mes



Corresponde a la suma del importe de las ventas del cliente durante un periodo mensual:



monto\_mes = SUM(sales\_amount)



En la implementación actual, sales\_amount es el alias de stg\_sales.sales\_total dentro del modelo intermedio.



frec\_bikes\_mes



Cuenta las líneas de venta cuya categoría normalizada es bikes:



SUM(CASE WHEN category\_clean = 'bikes' THEN 1 ELSE 0 END)



frec\_clothing\_mes



Cuenta las líneas de venta cuya categoría normalizada es clothing:



SUM(CASE WHEN category\_clean = 'clothing' THEN 1 ELSE 0 END)



frec\_accessories\_mes



Cuenta las líneas de venta cuya categoría normalizada es accessories:



SUM(CASE WHEN category\_clean = 'accessories' THEN 1 ELSE 0 END)



Las frecuencias representan el número de registros de venta asociados a cada categoría, no necesariamente el número de unidades vendidas. Para medir unidades, sería necesario agregar quantity en lugar de contar líneas.









##### **5. Capa Marts — catalog\_marts**





***5.1. Propósito de la capa***



La capa Marts contiene las estructuras de datos orientadas al consumo analítico y al modelado predictivo. Su objetivo es proporcionar dimensiones descriptivas, una tabla de hechos de ventas y una estructura consolidada de características de clientes.



Esta capa se compone de cinco tablas:



dim\_customers: dimensión de clientes con información demográfica, geográfica y socioeconómica.

dim\_products: dimensión de productos con información descriptiva y comercial.

fct\_sales: tabla de hechos con las transacciones comerciales y sus claves dimensionales.

customer\_360: conjunto de características agregadas por cliente para un periodo de análisis específico.

customer\_360\_historico: tabla Delta que conserva las características calculadas para distintos periodos de análisis.



Las tres primeras tablas implementan una estructura dimensional de tipo estrella. Las dos últimas están orientadas a la preparación de variables para la inferencia y el seguimiento temporal del comportamiento de compra.



***5.2. Tabla dim\_customers***



Tabla física: catalog\_marts.dim\_customers



Modelo dbt de origen: stg\_customers



Granularidad: un registro por cliente, suponiendo que customer\_id sea único en la tabla de origen.



Propósito: proporcionar una dimensión descriptiva de clientes para los análisis comerciales y las relaciones con la tabla de hechos.



Diccionario de campos



|Campo<br />|Tipo de dato esperado<br />|Descripción / regla<br />|
|-|-|-|
|customer\_id<br />signup\_date<br />city<br />state<br />country<br />segment<br />birth\_date<br />gender<br />marital\_status<br />yearly\_income<br />total\_children<br />children\_at\_home<br />education<br />occupation<br />house\_owner<br />number\_cars|INT<br />DATE<br />STRING<br />STRING<br />STRING<br />STRING<br />DATE<br />STRING<br />STRING<br />DECIMAL(15,2)<br />INT<br />INT<br />STRING<br />STRING<br />INT<br />INT|Identificador del cliente.<br />Fecha de primera compra registrada.<br />Ciudad de residencia.<br />Estado o provincia.<br />País o región geográfica.<br />Segmento derivado de la presencia de hijos en el hogar.<br />Fecha de nacimiento del cliente.<br />Género registrado.<br />Estado civil registrado.<br />Ingreso anual del cliente.<br />Número total de hijos.<br />Número de hijos que viven en el hogar.<br />Nivel educativo.<br />Ocupación.<br />Indicador de propiedad de vivienda.<br />Número de automóviles que posee el cliente.|
|edad\_cliente<br /><br />fecha\_carga\_mart|Numérico<br /><br />Tipo temporal devuelto por current\_timestamp()|Edad aproximada calculada a partir de la fecha de nacimiento y la fecha actual.<br /><br />Momento de procesamiento de la dimensión en la capa Marts.|







**Transformaciones aplicadas**



La tabla parte de stg\_customers y conserva sus campos originales. Se añade la variable edad\_cliente, calculada mediante la expresión:



datediff(current\_date(), birth\_date) / 365



Esta operación calcula la diferencia entre la fecha actual y la fecha de nacimiento en días, y la divide entre 365 para obtener una edad aproximada en años.



También se incorpora fecha\_carga\_mart mediante current\_timestamp() para registrar el momento de procesamiento del modelo.



**Consideraciones**



edad\_cliente representa una edad aproximada y puede diferir en un año de la edad cronológica exacta, ya que no considera de forma explícita los años bisiestos ni si el cumpleaños ya ocurrió durante el año actual.



Al utilizar current\_date(), la edad puede variar entre ejecuciones del modelo, aunque la fecha de nacimiento permanezca constante.

Si se necesita reproducibilidad histórica, conviene calcular la edad respecto de una fecha de referencia asociada al periodo de análisis.

En la implementación actual de customer\_360, las características demográficas se obtienen directamente de stg\_customers; por tanto, edad\_cliente no forma parte de las variables generadas para LightGBM.

La tabla no implementa explícitamente control de cambios históricos de atributos del cliente. Su comportamiento dependerá de la estrategia de materialización y actualización definida para el modelo dbt.







***5.3. Tabla dim\_products***



Tabla física: catalog\_marts.dim\_products



Modelo dbt de origen: stg\_products



Granularidad: un registro por producto, suponiendo que product\_id sea único en Staging.



Propósito: proporcionar atributos descriptivos de los productos comercializados para su utilización en consultas analíticas y relaciones con la tabla de hechos.



Diccionario de campos





|Campo<br />|Tipo de dato esperado<br />|Descripción / regla<br />|
|-|-|-|
|product\_id<br />category<br />subcategory<br />brand<br />description<br />price<br />fecha\_carga\_mart<br />|INT<br />STRING<br />STRING<br />STRING<br />STRING<br />DECIMAL(10,2)<br />Tipo temporal devuelto por current\_timestamp()<br />|Identificador del producto.<br />Categoría comercial del producto.<br />Subcategoría comercial del producto.<br />Nombre del modelo asociado al producto.<br />Nombre descriptivo del producto.<br />Precio de referencia del producto, con el tratamiento de nulos definido en Staging.<br />Momento de procesamiento de la dimensión en Marts.<br />|







Transformaciones aplicadas



La tabla conserva los campos de stg\_products y añade fecha\_carga\_mart.



No se incorporan nuevas transformaciones de negocio en este modelo. Los ajustes de precios y el tratamiento de nulos proceden de las capas anteriores.



Consideraciones

El campo price debe interpretarse como el precio de referencia transformado por el proyecto, no necesariamente como el precio original de AdventureWorksDW2019.

La unicidad de product\_id es necesaria para evitar duplicaciones al unir esta dimensión con fct\_sales.

Las categorías y subcategorías deben mantener valores consistentes para garantizar la correcta clasificación de las ventas.





***5.4. Tabla fct\_sales***



Tabla física: catalog\_marts.fct\_sales



Modelo dbt de origen: stg\_sales, dim\_customers y dim\_products.



Granularidad: una fila por línea de venta que tenga correspondencia con las dimensiones de clientes y productos.



Propósito: representar las transacciones comerciales en un modelo dimensional, permitiendo relacionar los importes y las cantidades vendidas con sus respectivos clientes, productos y periodos.



Diccionario de campos



|Campo<br />|Tipo de dato esperado<br />|Descripción / regla<br />|
|-|-|-|
|sale\_id<br />customer\_id<br />product\_id<br />fecha\_venta<br />codmes<br />quantity<br />unit\_price<br />sales\_total<br />fecha\_carga\_mart<br />|STRING<br />INT<br />INT<br />DATE<br />INT<br />INT<br />DOUBLE<br />DOUBLE<br />Tipo temporal devuelto por current\_timestamp()<br />|Identificador de la línea de venta.<br />Clave de relación con dim\_customers.<br />Clave de relación con dim\_products.<br />Fecha de la transacción, procedente de stg\_sales.timestamp.<br />Código de periodo mensual en formato AAAAMM.<br />Cantidad vendida, con las reglas de limpieza de Staging.<br />Precio unitario ajustado en las capas anteriores.<br />Importe de la línea de venta procedente de stg\_sales.sales\_total.<br />Momento de procesamiento de la tabla de hechos.<br />|









Transformaciones aplicadas



1\. Selección de la fuente transaccional



Se utiliza stg\_sales como fuente principal, conservando los identificadores, la fecha de venta, la cantidad, el precio unitario y el importe calculado.



2\. Generación del código mensual



El campo codmes se obtiene mediante:



year(timestamp) \* 100 + month(timestamp)



Por ejemplo, una venta de febrero de 2026 recibe el código 202602.



3\. Validación de correspondencias dimensionales



Se realizan uniones internas (INNER JOIN) con dim\_customers y dim\_products.



Estas uniones restringen la tabla de hechos a las ventas cuyos clientes y productos tienen correspondencia en las respectivas dimensiones.



4\. Registro de procesamiento



Se añade fecha\_carga\_mart mediante current\_timestamp().



Consideraciones

Las ventas sin correspondencia en alguna de las dimensiones quedan excluidas por las uniones internas. Es recomendable medir cuántos registros se descartan antes de considerar esta exclusión como una regla de integridad satisfactoria.

Las dimensiones deben mantener la unicidad de sus claves para evitar que una línea de venta se multiplique durante las uniones.

codmes facilita las uniones y agregaciones temporales, pero no sustituye una dimensión calendario si en el futuro se requieren atributos como trimestre, semana, feriado o día laborable.

El importe sales\_total conserva las reglas de cálculo de Staging. Su consistencia depende de que se calcule a partir de los campos finales de cantidad y precio unitario.

La estructura se aproxima a un modelo estrella con dos dimensiones descriptivas y una tabla de hechos. No se implementa una clave sustituta adicional para las dimensiones ni una estrategia explícita de historización de sus atributos.







***5.5. Tabla customer\_360***





Tabla física: catalog\_marts.customer\_360



Modelos dbt de origen:



int\_customer\_monthly\_aggregates

stg\_sales

stg\_customers



Granularidad: un registro por cliente incluido en el universo de clientes activos para un periodo de análisis.



Propósito: generar una matriz de características de comportamiento de compra y atributos socioeconómicos para alimentar el modelo predictivo LightGBM.



A diferencia de las dimensiones y la tabla de hechos, esta tabla está diseñada para proporcionar una representación consolidada del cliente en una fecha de corte determinada.



El periodo de análisis se controla mediante la variable dbt codmes\_inferencia. Cuando no se proporciona un valor, el modelo utiliza 202602 como valor predeterminado.



Ejemplo de ejecución:



dbt run --select customer\_360 --vars "{'codmes\_inferencia': 202602}"



***5.5.1. Parámetros temporales***



El modelo recibe un código mensual en formato AAAAMM y calcula dos ventanas temporales.



Ventana	Inicio	Fin	Uso

Ventana de 6 meses	Cinco meses antes del periodo de corte	Periodo de corte, incluido	Promedio mensual del importe y frecuencias de accesorios y ropa.

Ventana de 2 meses	Mes anterior al periodo de corte	Periodo de corte, incluido	Promedio mensual de frecuencia de compra de bicicletas.



Para un periodo de corte 202602, las ventanas son:



Ventana de seis meses: septiembre de 2025 a febrero de 2026.

Ventana de dos meses: enero de 2026 a febrero de 2026.



Las ventanas incluyen el mes de corte. Por tanto, si el modelo se utiliza para una predicción realizada antes de finalizar ese mes, deberá revisarse la fecha real de disponibilidad de los datos para evitar fuga de información.



***5.5.2. Construcción de la población analizada***



El modelo define el universo de clientes mediante la tabla int\_customer\_monthly\_aggregates.



Se incluyen los clientes que cumplen simultáneamente estas condiciones:



Presentan registros dentro de la ventana de seis meses, incluido el periodo de corte.

Tienen monto\_mes > 0 en al menos uno de los meses considerados.



Por consiguiente, customer\_360 no representa necesariamente a todos los clientes registrados en stg\_customers. Los clientes sin actividad comercial positiva durante la ventana no aparecen en el resultado.



**5.5.3. Diccionario de características**



El resultado final contiene las siguientes columnas, ordenadas para facilitar su consumo por el modelo LightGBM.



|Campo<br />|Tipo lógico esperado<br />|Descripción / regla<br />|
|-|-|-|
|monto\_prom\_6m<br />|Numérico<br />|Promedio mensual del importe agregado durante la ventana de seis meses. Los meses sin registros contribuyen como cero al numerador y se consideran dentro del divisor fijo de seis.<br />|
|frec\_accessories\_prom\_6m<br />|DOUBLE<br />|Promedio mensual de líneas de venta de accesorios durante los seis meses.<br />|
|recencia\_dias<br />|Entero o tipo numérico equivalente<br />|Días transcurridos entre el último día del periodo de corte y la última fecha de compra disponible hasta ese periodo.<br />|
|yearly\_income<br />|DOUBLE<br />|Ingreso anual del cliente convertido a tipo numérico. Los nulos se reemplazan por cero.<br />|
|frec\_clothing\_prom\_6m<br />|DOUBLE<br />|Promedio mensual de líneas de venta de ropa durante los seis meses.<br />|
|children\_at\_home<br />|Entero o tipo numérico equivalente<br />|Número de hijos en el hogar. Los nulos se reemplazan por cero.<br />|
|total\_children<br />|Entero o tipo numérico equivalente<br />|Número total de hijos. Los nulos se reemplazan por cero.|
|number\_cars<br />|Entero o tipo numérico equivalente<br />|Número de automóviles. Los nulos se reemplazan por cero.<br />|
|frec\_bikes\_prom\_2m<br />|DOUBLE<br />|Promedio mensual de líneas de venta de bicicletas durante los dos meses de la ventana de corte.<br />|
|customer\_id<br />|INT<br />|Identificador del cliente. Se conserva para trazabilidad y asociación de predicciones.<br />|
|periodo\_analisis<br />|INT<br />|Periodo de corte utilizado para calcular las características, en formato AAAAMM.<br />|







***5.5.4. Reglas de cálculo***



Importe promedio mensual de seis meses



Se suman los importes mensuales del cliente dentro de la ventana y se divide el resultado entre seis:



monto\_prom\_6m = SUM(monto\_mes) / 6.0



Los meses sin actividad no aparecen explícitamente en la tabla intermedia. Sin embargo, la expresión de agregación los trata como cero al dividir por un número fijo de seis meses.



Frecuencia promedio mensual de accesorios



Se suman las frecuencias mensuales de la categoría accessories y se divide entre seis:



frec\_accessories\_prom\_6m = SUM(frec\_accessories\_mes) / 6.0



Frecuencia promedio mensual de ropa



Se suman las frecuencias mensuales de la categoría clothing y se divide entre seis:



frec\_clothing\_prom\_6m = SUM(frec\_clothing\_mes) / 6.0



Frecuencia promedio mensual de bicicletas



Se suman las frecuencias mensuales de la categoría bikes durante el mes anterior y el mes de corte, y se divide entre dos:



frec\_bikes\_prom\_2m = SUM(frec\_bikes\_mes) / 2.0



Recencia de compra



La última compra se determina a partir de stg\_sales, considerando todas las transacciones cuya fecha pertenece a un periodo menor o igual al periodo de corte.



El cálculo es:



recencia\_dias = datediff(ultimo\_dia\_del\_periodo, ultima\_fecha\_compra)



Esta característica mide cuántos días han transcurrido desde la última compra hasta el final del mes de corte. No equivale a los días transcurridos desde la fecha actual.



Variables socioeconómicas



El ingreso anual se convierte a DOUBLE; los valores nulos se sustituyen por cero. Los contadores de hijos y automóviles también se completan con cero cuando no existe un valor registrado.





**5.6 Tabla customer\_360\_historico**



***5.6.1. Descripción general***



Tabla física: catalog\_marts.customer\_360\_historico



Fuente: ejecuciones sucesivas del modelo dbt customer\_360.



Formato de almacenamiento: Delta Lake.



Granularidad: un registro por combinación de customer\_id y periodo\_analisis, según la clave utilizada en el proceso de actualización.



Propósito: conservar las características de los clientes a través del tiempo, permitiendo reconstruir las matrices de inferencia correspondientes a diferentes periodos de análisis.



A diferencia de customer\_360, que se genera para un único periodo de corte por ejecución, customer\_360\_historico acumula los resultados de distintos periodos. Esto permite analizar la evolución de las características, reproducir escenarios históricos y disponer de registros asociados a las predicciones realizadas en diferentes fechas.



***5.6.2. Diccionario de campos***



La tabla histórica conserva las columnas generadas por customer\_360.





|Campo<br />|Tipo lógico esperado<br />|Descripción / regla<br />|
|-|-|-|
|monto\_prom\_6m<br />|Numérico<br />|Promedio mensual de importe en la ventana de seis meses correspondiente al periodo de análisis.|
|frec\_accessories\_prom\_6m<br />|DOUBLE<br />|Frecuencia promedio mensual de líneas de venta de accesorios en los seis meses previos y el mes de corte.|
|recencia\_dias<br />|Entero o tipo numérico equivalente<br />|Días desde la última compra hasta el final del periodo de análisis.|
|yearly\_income<br />|DOUBLE<br />|Ingreso anual del cliente utilizado en la matriz de características.|
|frec\_clothing\_prom\_6m<br />|DOUBLE<br />|Frecuencia promedio mensual de líneas de venta de ropa durante la ventana de seis meses.|
|children\_at\_home<br />|Entero o tipo numérico equivalente<br />|Número de hijos en el hogar.|
|total\_children<br />|Entero o tipo numérico equivalente<br />|Número total de hijos.|
|number\_cars<br />|Entero o tipo numérico equivalente<br />|Número de automóviles.<br />|
|frec\_bikes\_prom\_2m<br />|DOUBLE<br />|Frecuencia promedio mensual de líneas de venta de bicicletas durante la ventana de dos meses.<br />|
|customer\_id<br />|INT<br />|Identificador del cliente.|
|periodo\_analisis<br />|INT<br />|Periodo de corte al que corresponden las características.<br />|





Los tipos de datos de la tabla histórica se heredan del resultado de customer\_360 durante su creación y actualización.





##### **6. Capa Analytics — catalog\_analytics**



**6.1. Propósito de la capa**



La capa Analytics contiene tablas orientadas al análisis de resultados, seguimiento de clientes y evaluación de predicciones del modelo de Machine Learning.



Actualmente, esta capa contiene la tabla sales\_customers\_seguimiento, que integra tres componentes principales:



Características predictoras: variables demográficas y de comportamiento de compra procedentes del histórico de customer\_360.

Variable objetivo (target): indica si el cliente realizó al menos una compra durante los seis meses posteriores al periodo de análisis, siempre que exista suficiente información histórica para observar esa ventana.

Predicción (prediction): score numérico generado mediante un modelo LightGBM registrado en MLflow.



Esta estructura permite analizar las predicciones por cliente y periodo, contrastarlas con los resultados observados y preparar evaluaciones retrospectivas del modelo.



**6.2. Tabla sales\_customers\_seguimiento**



Tabla física: catalog\_analytics.sales\_customers\_seguimiento



Formato de almacenamiento: Delta Lake.



Fuentes directas:



catalog\_marts.customer\_360\_historico

catalog\_intermediate.int\_customer\_monthly\_aggregates

Modelo LightGBM registrado en MLflow.



Granularidad esperada: un registro por combinación de customer\_id y periodo\_analisis, sujeto a la unicidad de las fuentes y a las reglas de construcción de la tabla.



Propósito: consolidar las características de los clientes, el resultado observado de recompra cuando ya puede determinarse y el score generado por el modelo predictivo.



**6.3. Diccionario de campos**



|Campo<br />|Tipo lógico esperado<br />|Descripción / regla<br />|
|-|-|-|
|customer\_id<br />|INT<br />|Identificador del cliente. Permite asociar los resultados con las dimensiones y los registros históricos.<br />|
|periodo\_analisis<br />|INT<br />|Periodo mensual de referencia de las características, expresado en formato AAAAMM.|
|monto\_prom\_6m<br />|DOUBLE<br />|Importe promedio mensual de ventas durante la ventana histórica de seis meses.|
|frec\_accessories\_prom\_6m<br />|DOUBLE<br />|Promedio mensual de líneas de venta de accesorios durante la ventana de seis meses.|
|recencia\_dias<br /><br />|Entero o tipo numérico equivalente<br />|Número de días transcurridos entre el final del periodo de análisis y la última compra registrada hasta ese periodo.<br />|
|yearly\_income<br />|DOUBLE<br />|Ingreso anual registrado para el cliente, con los valores nulos sustituidos por cero en la generación de características.<br />|
|children\_at\_home<br />|INT<br />|Número de hijos que viven en el hogar. Los valores nulos se sustituyen por cero en la generación de características.<br />|
|total\_children<br />|INT<br />|Número total de hijos del cliente. Los valores nulos se sustituyen por cero en la generación de características.<br />|
|number\_cars<br />|INT<br />|Número de automóviles que posee el cliente. Los valores nulos se sustituyen por cero en la generación de características.<br />|
|frec\_bikes\_prom\_2m<br /><br />|DOUBLE<br />|Promedio mensual de líneas de venta de bicicletas durante la ventana de dos meses.<br /><br />|
|target<br />|INT, admite nulos<br />|Variable objetivo de recompra. Toma el valor 1 si se registra al menos una compra en los seis meses posteriores al periodo de análisis, 0 si no se registra ninguna y NULL cuando la ventana todavía no puede considerarse observable.<br />|
|prediction<br />|DOUBLE<br />|Resultado de la inferencia del modelo de lightgbm para predecir si hay al menos una compra (1) de un cliente en los próximos 6 meses o no (0) en el periodo de mes analizado. <br />|









Los tipos indicados corresponden al esquema esperado de la tabla según la definición proporcionada.



**6.4. Construcción de la tabla**



El proceso de generación se divide en cuatro etapas.



Etapa 1. Lectura de fuentes y preparación temporal



Se leen las características históricas desde customer\_360\_historico y las agregaciones mensuales desde int\_customer\_monthly\_aggregates.



Se crean dos campos temporales auxiliares:



periodo\_fecha: primer día del mes correspondiente a periodo\_analisis.

codmes\_fecha: primer día del mes correspondiente a codmes.



Por ejemplo, el código mensual 202602 se convierte en la fecha 2026-02-01.



Esta conversión permite realizar operaciones temporales con funciones de Spark, como sumar o restar meses, y comparar periodos de forma consistente.



Asimismo, se identifica la última fecha mensual disponible en la tabla intermedia mediante el máximo de codmes\_fecha.



Esta fecha determina hasta qué momento existe información de ventas para calcular los resultados observados.



Etapa 2. Construcción de la variable objetivo



La variable objetivo se calcula utilizando las compras registradas después del periodo de análisis y hasta seis meses después.



Para cada combinación de cliente y periodo se consideran las transacciones que cumplen las condiciones siguientes:



Pertenecen al mismo cliente.

Su mes de venta es posterior al mes de análisis.

Su mes de venta es menor o igual al mes situado seis meses después del periodo de análisis.



El código implementa la siguiente condición temporal:



codmes\_fecha > periodo\_fecha AND codmes\_fecha <= add\_months(periodo\_fecha, 6)



Posteriormente, las transacciones se agrupan por customer\_id y periodo\_analisis.



La variable intermedia target\_t se define de la siguiente manera:



Condición	target\_t

Se encuentra al menos un registro de ventas en la ventana futura	1

No se encuentra ningún registro de ventas en la ventana futura	0



La condición se implementa contando los registros de ventas asociados al cliente dentro de la ventana. Por tanto, la variable indica si existe actividad comercial futura, no cuántas compras ni cuánto dinero gastó el cliente.



La etiqueta representa una recompra posterior al periodo de análisis y no incluye las ventas correspondientes al mes de corte.



Etapa 3. Tratamiento de periodos no observables



No todos los periodos históricos cuentan con seis meses completos de información posterior. Por ello, el proceso identifica los periodos cuya ventana futura no puede considerarse completamente observable.



Se calcula una fecha de referencia restando cinco meses al primer día del último mes disponible en las ventas:



fecha\_inicio\_no\_observable = add\_months(max\_fecha\_ventas, -5)



Después, se asigna NULL a target cuando se cumple la condición:



periodo\_analisis >= date\_format(fecha\_inicio\_no\_observable, "yyyyMM")



Para los periodos anteriores se conserva el resultado calculado en target\_t.



Esta regla busca distinguir entre dos situaciones diferentes:



Cliente sin recompra observada: target = 0, cuando el periodo se considera evaluable y no existen registros de compras en la ventana futura.

Resultado todavía no observable: target = NULL, cuando el periodo se encuentra dentro del tramo final de la historia disponible.



Esta distinción es importante porque un periodo sin seis meses completos de seguimiento no debe interpretarse automáticamente como una ausencia de recompra.



Nota metodológica: la frontera implementada debe validarse frente a la disponibilidad real de los seis meses futuros. El cálculo resta cinco meses al último mes disponible, por lo que conviene revisar si esa frontera es suficiente para garantizar una ventana completa bajo la convención temporal utilizada.



Etapa 4. Generación del score predictivo



El modelo se carga desde MLflow utilizando el identificador de ejecución configurado en el proceso.



La integración se realiza mediante mlflow.pyfunc.spark\_udf, que permite aplicar el modelo sobre un DataFrame de Spark.



Las nueve variables de entrada se proporcionan en el siguiente orden:



monto\_prom\_6m

frec\_accessories\_prom\_6m

recencia\_dias

yearly\_income

frec\_clothing\_prom\_6m

children\_at\_home

total\_children

number\_cars

frec\_bikes\_prom\_2m



El resultado de la función se almacena en la columna prediction, de tipo DOUBLE.



Finalmente, el DataFrame se escribe en formato Delta mediante una operación overwrite con overwriteSchema = true.



Esto significa que cada ejecución reemplaza el contenido anterior de catalog\_analytics.sales\_customers\_seguimiento con el nuevo resultado completo y permite que el esquema de la tabla se actualice si cambia el esquema de salida.















