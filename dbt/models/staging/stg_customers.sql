with source_data as (
    select * from {{ source('databricks_raw', 'customers') }}
)

select
    cast(customer_id as int) as customer_id,
    cast(signup_date as date) as signup_date,
    cast(city as string) as city,
    cast(state as string) as state,
    cast(country as string) as country,
    cast(segment as string) as segment,
    cast(birth_date as date) as birth_date,
    cast(gender as string) as gender,
    cast(marital_status as string) as marital_status,
    cast(yearly_income as decimal(15,2)) as yearly_income,
    cast(total_children as int) as total_children,
    cast(children_at_home as int) as children_at_home,
    cast(education as string) as education,
    cast(occupation as string) as occupation,
    cast(house_owner as int) as house_owner,
    cast(number_cars as int) as number_cars,
    current_timestamp() as fecha_ingesta_staging
from source_data