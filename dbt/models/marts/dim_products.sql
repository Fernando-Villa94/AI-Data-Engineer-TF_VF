with stg_products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    category,
    subcategory,
    brand,
    description,
    price,
    current_timestamp() as fecha_carga_mart
from stg_products
