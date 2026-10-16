-- Trigger dbt workflow test
{{ config(materialized='table') }}

SELECT
    CAST(carat AS DOUBLE)     AS carat,
    UPPER(TRIM(cut))          AS cut,
    UPPER(TRIM(color))        AS color,
    UPPER(TRIM(clarity))      AS clarity,
    CAST(price AS INT)        AS price
FROM {{ ref('diamonds') }}
WHERE price > 0
  AND carat > 0