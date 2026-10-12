with source_data as (
    select * from {{ source('databricks_raw', 'products') }}
)

select
    cast(product_id as int) as product_id,
    cast(category as string) as category,
    cast(subcategory as string) as subcategory,
    cast(brand as string) as brand,
    cast(description as string) as description,
    COALESCE(CAST(price AS DECIMAL(10,2)), 0.00) as price,
    -- Regla 6: Frescura (Fecha de procesamiento en esta nueva capa)
    current_timestamp() as fecha_ingesta_staging
from source_data