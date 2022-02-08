{% macro delete_outdated_tables(schema) %} 
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {% do exceptions.raise_compiler_error('"schema" must be a string or a list') %}
  {% endif %}

  {% call statement('get_outdated_tables', fetch_result=True) %}
    select current.schema_name,
           current.ref_name,
           current.ref_type
    from (
      select schemaname as schema_name, 
             tablename  as ref_name, 
             'table'    as ref_type
      from pg_catalog.pg_tables pt 
      where schemaname in (
        {%- if schema is iterable and (var is not string and var is not mapping) -%}
          {%- for s in schema -%}
            '{{ s }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- elif schema is string -%}
          '{{ schema }}'
        {%- endif -%}
      )
      union all
      select schemaname as schema_name, 
             viewname   as ref_name, 
             'view'     as ref_type
      from pg_catalog.pg_views
        where schemaname in (
        {%- if schema is iterable and (var is not string and var is not mapping) -%}
          {%- for s in schema -%}
            '{{ s }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- elif schema is string -%}
          '{{ schema }}'
        {%- endif -%}
      )) as current
    left join (values
      {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                    + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %} 
        ('{{node.schema}}', '{{node.name}}'){% if not loop.last %},{% endif %}
      {%- endfor %}
    ) as desired (schema_name, ref_name) on desired.schema_name = current.schema_name
                                        and desired.ref_name    = current.ref_name
    where desired.ref_name is null
  {% endcall %}

  {%- for to_delete in load_result('get_outdated_tables')['data'] %} 
    {% call statement() -%}
      {% do log('dropping ' ~ to_delete[2] ~ ' "' ~ to_delete[0] ~ '.' ~ to_delete[1], info=true) %}
      drop {{ to_delete[2] }} if exists "{{ to_delete[0] }}"."{{ to_delete[1] }}" cascade;
    {%- endcall %}
  {%- endfor %}

{% endmacro %}