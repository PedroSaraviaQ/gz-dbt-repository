WITH margin_per_sale AS (
    SELECT 
        products_id, 
        date_date, 
        orders_id,
        revenue, 
        quantity, 
        CAST(purchase_price AS FLOAT64), 
        ROUND(s.quantity*CAST(p.purchase_price AS FLOAT64),2) AS purchase_cost,
        s.revenue - ROUND(s.quantity*CAST(p.purchase_price AS FLOAT64),2) AS margin
    FROM {{ref("stg_raw__sales")}} s
    LEFT JOIN {{ref("stg_raw__product")}} p 
    USING (products_id)
)

SELECT
    orders_id
    ,date_date
    ,ROUND(SUM(revenue),2) AS revenue
    ,CAST(SUM(quantity) AS FLOAT64) AS quantity
    ,ROUND(SUM(purchase_cost),2) AS purchase_cost
    ,ROUND(SUM(revenue)-SUM(purchase_cost),2) AS margin
FROM margin_per_sale
GROUP BY
    orders_id
    ,date_date
ORDER BY orders_id DESC

