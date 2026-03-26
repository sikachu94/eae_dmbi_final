![alt](https://keystoneacademic-res.cloudinary.com/image/upload/dpr_auto/f_auto/q_auto/v1/element/24/244366_AF_EAEBSB_LOGO_RGB_ROJO.png)

# Big Data & Analytics '26

# Data Management for BI

# Final Assignment

By:
Omar Ahmed
github.com/sikachu94/eae_dmbi_final

## Data Model Design

### Overview

This file is meant to explain all the different aspects required for delivery for the Data Management for BI final assignment.
The data warehouse for this project follows a **star schema** design. At the centre sits a single fact table — `spf_sales_fact` — which captures every lead and its journey through the solar panel sales funnel, and two dimension tables — `spf_zipcode_dim` and `spf_weather_dim` — extend that central table with geographic and meteorological context via a shared `zipcode` key.

---

### Schema Diagram

![erd](https://imgur.com/KabwrWc.png)

---

### Why a Star Schema?

The raw source data arrives as a flat file (`sale_phases_funnel.csv`) with every attribute crammed into a single row per lead. A star schema was chosen to restructure this for analytical use for the following reasons.

**Query simplicity.** A star schema exposes clean, denormalised dimensions. Business questions like "what is the average installation price per province?" can be answered with a single JOIN between the fact table and the zipcode dimension, rather than navigating a complex web of normalised tables.

**BI tool compatibility.** Tools like Metabase, Tableau, or Power BI are built around the concept of a fact table joined to dimensions. A star schema maps directly to how those tools define metrics and groupings, meaning less configuration overhead.

**Performance.** Because the dimensions are denormalised, there are fewer joins at query time. On a dataset of ~35,000 leads this is not a bottleneck today, but it is the right foundation if the data grows significantly.

**Separation of concerns.** Keeping geographic data in `spf_zipcode_dim` and weather data in `spf_weather_dim` means that if the source CSVs for either of those are updated independently, only the relevant dimension table needs to be reloaded. The fact table is untouched.

---

### Primary / Foreign Key Summary

| Table             | Primary Key       | Foreign Key                                                                  |
| :---------------- | :---------------- | :--------------------------------------------------------------------------- |
| `spf_sales_fact`  | `lead_id`         | `zipcode` → `spf_zipcode_dim.zipcode`, `zipcode` → `spf_weather_dim.zipcode` |
| `spf_zipcode_dim` | `zipcode`         |                                                                              |
| `spf_weather_dim` | `(zipcode, date)` |                                                                              |

### Naming Convention

All tables follow the convention `spf_{name}_{type}` where:

- `spf_` is the project prefix (sales phases funnel)
- `_fact` denotes the central fact table
- `_dim` denotes a dimension table

---

### Relationship Notes

The relationship between `spf_sales_fact` and `spf_weather_dim` is worth clarifying. While both tables share a `zipcode` column, there is no direct foreign key enforced in MySQL between them — the join is performed analytically at query time by matching `zipcode` and a chosen date column (e.g. `visit_date`).
The `zipcode` in the `spf_zipcode_dim`is also not related to the `zipcode` in`spf_weather_dim` this is to avoid a circular relationship in the model.

---

---

## Cleaning Operations

For the cleaning operations, it would be simpler to go through each table and the cleaning methods used, and the justification for these data transformations.

---

### spf_sales_fact

Cleaning operations applied:

1. Standardise column names to lowercase.
2. Remove duplicate leads (same lead_id appearing more than once).
3. Impute missing installation_peak_power_kw
4. Validate categorical fields financing_type and visiting_company
5. Parse all date columns to proper datetime types
6. Strip leading/trailing whitespace from string columns

The cleaning operations are a mix of standard data quality checks (dedupe, snakecase column names, removing whitespace, assign data types) with table specific data quality checks (impute and replace null values, validate categorical fields)

---

### spf_zipcode_dim

Cleaning operations applied:

1. Standardise column names to lowercase.
2. Remove rows where ZIPCODE is null – it is the primary key.
3. Remove duplicate zip codes (keep first occurrence).
4. Strip whitespace from string columns

For the zipcode table, the cleaning operations are mostly standard data quality checks - removing duplicates, stripping whitespace, and standardized column names.
The only table specific DQ check is for the nulls in the primary key, which will be dropped in this case to avoid any problems in query joins.

---

### spf_weather_dim

Cleaning operations applied:

1. Standardise column names to lowercase.
2. Parse the date column to proper datetime type.
3. Remove rows where both zipcode and date are null - primary keys couple
4. Remove exact duplicate rows

For the weather table, the cleaning operations are mostly standard data quality checks - removing duplicates, assigning date type, and standardized column names.
The only table specific DQ check is for the nulls in the primary key couple of date + zipcode, which will be dropped in this case to avoid any problems in query joins.

---

---

## Index Selection

---

Indexes were chosen based on the columns actually used in the business queries. No speculative indexing was added. This is a business-first approach rather than deeply technical one.

### `spf_sales_fact`

- `zipcode` - JOIN key to both dimension tables across multiple queries
- `financing_type` - Filtered and grouped in the cash sales and average price queries
- `contract_1_signature_date` - Grouped by month to find the top sales month

### `spf_zipcode_dim`

`province` - Grouped by in the avg power / price / leads per province query

### `spf_weather_dim`

No additional indexes needed. The composite primary key `(zipcode, date)` already covers it all indexing needs.

## SQL Queries

### Q1

Which are the top 5 KO reasons, and which percentage of total KO
represent each one?

```
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
```

![Imgur](https://imgur.com/XN8N4ih.png)

---

### Q2

```
SELECT
    DATE_FORMAT(project_validation_date, '%m-%Y') AS sale_month,
    COUNT(*) AS sales_count
FROM  spf_sales_fact
WHERE financing_type = 'cash'
  AND current_phase = 'Validated project'
  AND project_validation_date IS NOT NULL
GROUP BY sale_month
ORDER BY sales_count DESC
```

![Imgur](https://imgur.com/TnSLpJr.png)

---

### Q3

```
SELECT
    financing_type,
    ROUND(AVG(installation_price), 2)  AS avg_installation_price
FROM  spf_sales_fact
WHERE cusomer_type = 'Individual household'
  AND financing_type IS NOT NULL
GROUP BY financing_type
ORDER BY financing_type;
```

![Imgur](https://imgur.com/ZzO3JOi.png)

---

### Q4

```
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

```

![Imgur](https://imgur.com/40gBv5r.png)

---

### Q5

```
SELECT
    f.zipcode,
    ROUND(COUNT(CASE WHEN f.current_phase = 'Validated project' OR f.project_validation_date is not null THEN 1 END) / COUNT(*), 2) AS sales_conversion,
    ROUND(AVG(w.temperature), 2) AS avg_temperature
FROM spf_sales_fact f
LEFT JOIN  spf_weather_dim w ON f.zipcode = w.zipcode and f.project_validation_date = w.date
GROUP BY f.zipcode
HAVING COUNT(CASE WHEN f.current_phase = 'Validated project' THEN 1 END) > 0
ORDER BY sales_conversion DESC
```

![Imgur](https://imgur.com/PDtXSpF.png)
