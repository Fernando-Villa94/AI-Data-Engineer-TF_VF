with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

dim_customers as (
    select customer_id from {{ ref('dim_customers') }}
),

dim_products as (
    select product_id from {{ ref('dim_products') }}
)

select
    s.sale_id,
    s.customer_id,
    s.product_id,
    s.timestamp as fecha_venta,
    -- Clave inteligente temporal para joins de negocio YYYYMM
    year(s.timestamp) * 100 + month(s.timestamp) as codmes,
    s.quantity,
    s.unit_price,
    s.sales_total,
    current_timestamp() as fecha_carga_mart
from stg_sales s
-- Aseguramos la integridad analítica del Modelo Estrella
inner join dim_customers c on s.customer_id = c.customer_id
inner join dim_products p on s.product_id = p.product_id
