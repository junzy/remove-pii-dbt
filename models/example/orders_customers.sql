{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "magichaven",
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('orders_customer_ab3') }}
select
    _airbyte_orders_hashid,
    {{ adapter.quote('id') }},
    note,
    tags,
    {{ adapter.quote('state') }},
    currency,
    created_at,
    tax_exempt,
    updated_at,
    total_spent,
    orders_count,
    last_order_id,
    last_order_name,
    accepts_marketing,
    admin_graphql_api_id,
    multipass_identifier,
    marketing_opt_in_level,
    accepts_marketing_updated_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_customer_hashid
from {{ ref('orders_customer_ab3') }}
-- customer at orders/customer from {{ ref('orders') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}