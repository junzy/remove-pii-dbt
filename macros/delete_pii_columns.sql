{% macro delete_pii_columns(schema, columns, tables) %} 
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
 

{% endmacro %}§