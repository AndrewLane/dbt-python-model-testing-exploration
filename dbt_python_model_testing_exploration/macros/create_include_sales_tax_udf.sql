{% macro create_include_sales_tax_udf() %}

drop function if exists {{target.schema}}.include_sales_tax(float);

create function if not exists {{target.schema}}.include_sales_tax(amount float)
returns float
language python
runtime_version = '3.8'
handler = 'include_sales_tax'
as

$$

def include_sales_tax(amount):
    # assumes 7.5% tax
    return amount * 1.075
$$
;

{% endmacro %}