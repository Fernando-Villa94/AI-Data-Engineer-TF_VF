{% test es_mayor_o_igual_a(model, column_name, valor_minimo) %}

with validacion as (
    select
        {{ column_name }} as valor_columna
    from {{ model }}
)

select *
from validacion
where valor_columna < {{ valor_minimo }}

{% endtest %}