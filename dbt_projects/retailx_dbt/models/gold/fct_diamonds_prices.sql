{{ config(materialized='table') }}

SELECT
    cut,
    color,
    clarity,
    COUNT(*)              AS num_diamonds,
    ROUND(AVG(price), 2)  AS avg_price,
    MIN(price)            AS min_price,
    MAX(price)            AS max_price,
    ROUND(AVG(carat), 3)  AS avg_carat
FROM {{ ref('stg_diamonds') }}
GROUP BY cut, color, clarity