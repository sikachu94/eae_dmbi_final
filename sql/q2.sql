SELECT
    DATE_FORMAT(project_validation_date, '%m-%Y') AS sale_month,
    COUNT(*) AS sales_count
FROM  spf_sales_fact
WHERE financing_type = 'cash'
  AND current_phase = 'Validated project'
  AND project_validation_date IS NOT NULL
GROUP BY sale_month
ORDER BY sales_count DESC