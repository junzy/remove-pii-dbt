
  create view "postgres"."dbt_test"."customers_edited__dbt_tmp" as (
    ALTER TABLE magichaven.customers
DROP COLUMN IF EXISTS first_name;
  );