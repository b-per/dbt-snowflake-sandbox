
-- Use the `ref` function to select from other models


select
  {{ dbt_utils.star(ref('my_first_dbt_model')) }}
from {{ ref('my_first_dbt_model') }}