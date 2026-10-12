{# 
  Acepta el parámetro dinámico enviado por CLI:
  dbt run --select customer_360 --vars "{'codmes_inferencia': 202602}"
#}

{% set mes_corte = var('codmes_inferencia', 202602) | int %}

{# 
  CÁLCULOS EN JINJA PURO (Python):
  Convertimos el entero (Ej: 202602) a año y mes para restar los meses de las ventanas directamente.
#}
{% set anio_corte = (mes_corte // 100) | int %}
{% set mes_actual = (mes_corte % 100) | int %}

{# Lógica para restar 5 meses (Ventana de 6 meses) #}
{% set mes_inicio_6m_calculado = mes_actual - 5 %}
{% if mes_inicio_6m_calculado <= 0 %}
    {% set anio_6m = anio_corte - 1 %}
    {% set mes_6m = mes_inicio_6m_calculado + 12 %}
{% else %}
    {% set anio_6m = anio_corte %}
    {% set mes_6m = mes_inicio_6m_calculado %}
{% endif %}
{% set mes_inicio_6m = (anio_6m * 100 + mes_6m) | int %}

{# Lógica para restar 1 mes (Ventana de 2 meses) #}
{% set mes_inicio_2m_calculado = mes_actual - 1 %}
{% if mes_inicio_2m_calculado <= 0 %}
    {% set anio_2m = anio_corte - 1 %}
    {% set mes_2m = mes_inicio_2m_calculado + 12 %}
{% else %}
    {% set anio_2m = anio_corte %}
    {% set mes_2m = mes_inicio_2m_calculado %}
{% endif %}
{% set mes_inicio_2m = (anio_2m * 100 + mes_2m) | int %}



with monthly_base as (
    select * from {{ ref('int_customer_monthly_aggregates') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

sales_dates as (
    select 
        customer_id,
        `timestamp` as sale_date,
        year(`timestamp`) * 100 + month(`timestamp`) as codmes
    from {{ ref('stg_sales') }}
),

-- PASO 1: LIMITAR EL UNIVERSO ESTRICTAMENTE A LOS 6 MESES PREVIOS AL CORTE
universo_activos as (
    select distinct customer_id
    from monthly_base
    where codmes between {{ mes_inicio_6m }} and {{ mes_corte }}
      and monto_mes > 0
),

-- PASO 2: AGREGACIÓN DE MÉTRICAS HISTÓRICAS DE LAS VENTANAS
historical_features as (
    select
        m.customer_id,
        
        -- Window 6m: Monto promedio mensual
        coalesce(sum(case when m.codmes between {{ mes_inicio_6m }} and {{ mes_corte }} then m.monto_mes else 0 end), 0) / 6.0 as monto_prom_6m,
        
        -- Window 6m: Frecuencia promedio de accesorios
        cast(coalesce(sum(case when m.codmes between {{ mes_inicio_6m }} and {{ mes_corte }} then m.frec_accessories_mes else 0 end), 0) as double) / 6.0 as frec_accessories_prom_6m,
        
        -- Window 6m: Frecuencia promedio de ropa
        cast(coalesce(sum(case when m.codmes between {{ mes_inicio_6m }} and {{ mes_corte }} then m.frec_clothing_mes else 0 end), 0) as double) / 6.0 as frec_clothing_prom_6m,

        -- Window 2m: Frecuencia promedio de bicicletas (mes de corte y anterior)
        cast(coalesce(sum(case when m.codmes between {{ mes_inicio_2m }} and {{ mes_corte }} then m.frec_bikes_mes else 0 end), 0) as double) / 2.0 as frec_bikes_prom_2m

    from monthly_base m
    inner join universo_activos u on m.customer_id = u.customer_id
    group by m.customer_id
),

-- PASO 3: CÁLCULO DE LA RECENCIA EN DÍAS (Hasta el mes de corte solicitado)
recency_calculation as (
    select
        customer_id,
        max(sale_date) as ultima_fecha_compra
    from sales_dates
    where codmes <= {{ mes_corte }}
    group by customer_id
),

-- PASO 4: ENSAMBLE FINAL CON SOCIODEMOGRÁFICOS Y FORMATO DE ENTRADA LIGHTGBM
final_features as (
    select
        h.customer_id,
        {{ mes_corte }} as periodo_analisis,
        
        -- Métricas agregadas de ventanas
        h.monto_prom_6m,
        h.frec_accessories_prom_6m,
        
        -- Recencia nativa en Databricks (Diferencia de días entre fin de mes analizado y última compra)
        datediff(
            last_day(to_date(cast({{ mes_corte }} as string), 'yyyyMM')), 
            r.ultima_fecha_compra
        ) as recencia_dias,
        
        -- Demográficos con reemplazo de nulos
        coalesce(cast(c.yearly_income as double), 0.0) as yearly_income,
        h.frec_clothing_prom_6m,
        coalesce(c.children_at_home, 0) as children_at_home,
        coalesce(c.total_children, 0) as total_children,
        coalesce(c.number_cars, 0) as number_cars,
        h.frec_bikes_prom_2m
        
    from historical_features h
    left join recency_calculation r on h.customer_id = r.customer_id
    left join customers c on h.customer_id = c.customer_id
)

-- ESTRUCTURA FINAL ORDENADA REQUERIDA POR TU MATRIZ DE LIGHTGBM
select 
    monto_prom_6m,
    frec_accessories_prom_6m,
    recencia_dias,
    yearly_income,
    frec_clothing_prom_6m,
    children_at_home,
    total_children,
    number_cars,
    frec_bikes_prom_2m,
    -- Datos adicionales útiles para trazabilidad
    customer_id,
    periodo_analisis
from final_features