-- This test fails if any diamond has a negative or zero price
SELECT *
FROM {{ ref('stg_diamonds') }}
WHERE price <= 0