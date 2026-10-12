with sales as (
    select * from {{ ref('stg_sales') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

sales_with_categories as (
    select
        s.customer_id,
        -- Generamos el codmes numérico (Ej: 202610)
        year(s.timestamp) * 100 + month(s.timestamp) as codmes,
        s.timestamp as sale_date,
        s.sales_total as sales_amount,
        -- Traemos la categoría limpia
        lower(trim(coalesce(p.category, 'unknown'))) as category_clean
    from sales s
    left join products p 
        on s.product_id = p.product_id
),

monthly_aggregates as (
    select
        customer_id,
        codmes,
        sum(sales_amount) as monto_mes,
        -- Frecuencias condicionales por categoría por mes
        sum(case when category_clean = 'bikes' then 1 else 0 end) as frec_bikes_mes,
        sum(case when category_clean = 'clothing' then 1 else 0 end) as frec_clothing_mes,
        sum(case when category_clean = 'accessories' then 1 else 0 end) as frec_accessories_mes
    from sales_with_categories
    group by 1, 2
)

select * from monthly_aggregates