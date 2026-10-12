with source_data as (
    select * from {{ source('databricks_raw', 'sales') }}
)

select
    cast(sale_id as string) as sale_id,
    cast(customer_id as int) as customer_id,
    cast(product_id as int) as product_id,
    cast(timestamp as date) as timestamp,
    
    -- Regla 2: Validez (Evitar negativos en cantidad)
    case when cast(quantity as int) < 0 then 0 else cast(quantity as int) end as quantity,
    
    -- Regla 2: Validez (Evitar negativos en precio)
    case when cast(unit_price as double) < 0 then 0.00 else COALESCE(CAST(unit_price AS double), 0.00) end as unit_price,
    
    -- Regla 5: Consistencia (Multiplicación mandatoria)
    cast(quantity as int) * cast(unit_price as double) as sales_total,
    
    current_timestamp() as fecha_ingesta_staging
from source_data