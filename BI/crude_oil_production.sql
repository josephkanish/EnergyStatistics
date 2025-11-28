SELECT
  y.year,
  f.value_official AS production
FROM bi_db.fact_indicator_values f
JOIN bi_db.dim_indicator d ON f.indicator_key = d.indicator_key
JOIN bi_db.dim_year y ON f.year_key = y.year_key
WHERE d.indicator_code = '1.1'
ORDER BY y.year;
