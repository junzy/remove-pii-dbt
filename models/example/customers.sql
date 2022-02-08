{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "magichaven",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('customers_ab3') }}
select
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
    verified_email,
    default_address,
    last_order_name,
    accepts_marketing,
    admin_graphql_api_id,
    multipass_identifier,
    accepts_marketing_updated_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_customers_hashid
from {{ ref('customers_ab3') }}
-- customers from {{ source('magichaven', '_airbyte_raw_customers') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}
