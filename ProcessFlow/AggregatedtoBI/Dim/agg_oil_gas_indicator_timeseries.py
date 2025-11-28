from pyspark.sql import functions as F

agg_ts = spark.table("agg_db.oil_gas_indicator_timeseries")

dim_indicator = (
    agg_ts
    .select("indicator_code", "indicator_name", "section")
    .distinct()
    .withColumn("indicator_key", F.monotonically_increasing_id())
)

dim_year = (
    agg_ts
    .select("year")
    .distinct()
    .withColumn("year_key", F.monotonically_increasing_id())
)

dim_unit = (
    agg_ts
    .select("unit")
    .distinct()
    .withColumn("unit_key", F.monotonically_increasing_id())
)

dim_indicator.write.format("delta").mode("overwrite").saveAsTable("bi_db.dim_indicator")
dim_year.write.format("delta").mode("overwrite").saveAsTable("bi_db.dim_year")
dim_unit.write.format("delta").mode("overwrite").saveAsTable("bi_db.dim_unit")
