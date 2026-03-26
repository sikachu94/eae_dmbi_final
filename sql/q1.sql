SELECT
    ko_reason,
    COUNT(*) AS number_of_ko,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*)
        FROM   spf_sales_fact
        WHERE  ko_reason IS NOT NULL
          AND  ko_reason <> ''
    ), 2) AS pct_of_total_ko
FROM  spf_sales_fact
WHERE ko_reason IS NOT NULL
  AND ko_reason <> ''
GROUP BY ko_reason
ORDER BY number_of_ko DESC
LIMIT 5;