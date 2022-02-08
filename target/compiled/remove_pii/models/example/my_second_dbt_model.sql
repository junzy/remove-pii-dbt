-- Use the `ref` function to select from other models

select *
from "postgres"."dbt_test"."my_first_dbt_model"
where id = 1