from pyspark.sql import functions as F

l0 = spark.table("l0_db.oil_gas_indicator_values")

l1 = (
    l0
    .withColumn("is_year_valid", (F.col("year") >= 1960) & (F.col("year") <= 2100))
    .withColumn("is_value_valid", F.col("value").isNotNull() & (F.col("value") >= 0))
    .withColumn(
        "validation_status",
        F.when(~F.col("is_year_valid"), F.lit("INVALID_YEAR"))
         .when(~F.col("is_value_valid"), F.lit("INVALID_VALUE"))
         .otherwise(F.lit("OK"))
    )
)

l1.write.format("delta").mode("overwrite").saveAsTable("l1_db.oil_gas_indicator_values_validated")
