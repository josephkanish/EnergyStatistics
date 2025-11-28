from pyspark.sql import functions as F

dim_indicator = spark.table("bi_db.dim_indicator")
dim_year = spark.table("bi_db.dim_year")
dim_unit = spark.table("bi_db.dim_unit")

agg_ts = spark.table("agg_db.oil_gas_indicator_timeseries")

fact = (
    agg_ts
    .join(dim_indicator, on=["indicator_code", "indicator_name", "section"], how="left")
    .join(dim_year, on="year", how="left")
    .join(dim_unit, on="unit", how="left")
    .select(
        "indicator_key",
        "year_key",
        "unit_key",
        "value_official"
    )
)

fact.write.format("delta").mode("overwrite").saveAsTable("bi_db.fact_indicator_values")
