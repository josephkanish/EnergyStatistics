from pyspark.sql import functions as F

l1 = spark.table("l1_db.oil_gas_indicator_values_validated")

agg_timeseries = (
    l1
    .filter(F.col("validation_status") == "OK")
    .groupBy("indicator_code", "indicator_name", "year", "unit", "section")
    .agg(F.sum("value").alias("value_official"))
)

agg_timeseries_stats = (
    l1
    .filter(F.col("validation_status") == "OK")
    .groupBy("indicator_code", "indicator_name", "year", "unit", "section")
    .agg(F.sum("value").alias("value_official"))
)

agg_timeseries.write.format("delta").mode("overwrite").saveAsTable("agg_db.oil_gas_indicator_timeseries")
agg_timeseries_stats.write.format("delta").mode("overwrite").saveAsTable("agg_db.oil_gas_indicator_timeseries_stats")
