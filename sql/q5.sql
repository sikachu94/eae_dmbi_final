SELECT
    f.zipcode,
    ROUND(COUNT(CASE WHEN f.current_phase = 'Validated project' OR f.project_validation_date is not null THEN 1 END) / COUNT(*), 2) AS sales_conversion,
    ROUND(AVG(w.temperature), 2) AS avg_temperature
FROM spf_sales_fact f
LEFT JOIN  spf_weather_dim w ON f.zipcode = w.zipcode and f.project_validation_date = w.date
GROUP BY f.zipcode
HAVING COUNT(CASE WHEN f.current_phase = 'Validated project' THEN 1 END) > 0
ORDER BY sales_conversion DESC