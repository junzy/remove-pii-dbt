{% macro delete_pii_columns() %} 
{% set schema = "dbt_test" %}
{% set columns = ["first_name", "last_name", "email", "addresses", "verified_email", "name", "phone"] %}
{% set tables = ["customers", "orders_customer", "orders_customer_default_address", "customers_addresses", "customers_default_address", "draft_orders_customer", "draft_orders_customer_default_address", "abandoned_checkouts_customer", "abandoned_checkouts_customer_addresses"] %}
  {% do log('Input schema: ' ~ schema ~ ', columns: ' ~ columns ~ ', tables: ' ~ table , info=true) %}   
  {%- for table in tables -%}
      {% do log('altering table ' ~ schema ~ '.' ~ table , info=true) %}
      {% call statement() -%}
        ALTER TABLE {{schema}}.{{table}} 
        {%- for column in columns -%}
        {% raw %} {% endraw %}  
          DROP COLUMN IF EXISTS {{column}}
          {% if not loop.last %},{% endif %}
        {%- endfor -%}
        ;
        commit;
      {%- endcall %}
  {%- endfor -%}
  {% call statement() -%}
    ALTER TABLE {{schema}}.orders
    ADD COLUMN customer_id FLOAT(50);

    UPDATE {{schema}}.orders
    SET customer_id = CAST(js.customer -> 'id' as FLOAT(50))
    FROM {{schema}}.orders as js
    WHERE js.id = {{schema}}.orders.id;
  {%- endcall %}
{% endmacro %}§