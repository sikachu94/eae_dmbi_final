WITH zipcodes_leads_over_5 AS (
    SELECT
        zipcode,
        COUNT(*) AS n_leads_in_zip
    FROM  spf_sales_fact
    GROUP BY zipcode
    HAVING COUNT(*) > 5
)

SELECT
    z.province,
    ROUND(AVG(f.installation_peak_power_kw), 2) AS avg_peak_power_kw,
    ROUND(AVG(f.installation_price), 2) AS avg_installation_price,
    COUNT(f.lead_id) AS n_leads
FROM spf_sales_fact f
INNER JOIN zipcodes_leads_over_5  zl ON f.zipcode = zl.zipcode
INNER JOIN spf_zipcode_dim z  ON f.zipcode = z.zipcode
GROUP BY z.province
ORDER BY z.province