SELECT
    financing_type,
    ROUND(AVG(installation_price), 2)  AS avg_installation_price
FROM  spf_sales_fact
WHERE cusomer_type = 'Individual household'
  AND financing_type IS NOT NULL
GROUP BY financing_type
ORDER BY financing_type;