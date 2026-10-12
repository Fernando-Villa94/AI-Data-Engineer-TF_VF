with stg_customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    signup_date,
    city,
    state,
    country,
    segment,
    birth_date,
    gender,
    marital_status,
    yearly_income,
    total_children,
    children_at_home,
    education,
    occupation,
    house_owner,
    number_cars,
    -- Calculamos la edad actual como métrica demográfica de valor añadido
    datediff(current_date(), birth_date) / 365 as edad_cliente,
    current_timestamp() as fecha_carga_mart
from stg_customers
