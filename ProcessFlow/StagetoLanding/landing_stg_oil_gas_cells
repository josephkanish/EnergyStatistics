from pyspark.sql.window import Window

cells = spark.table("stg_db.stg_oil_gas_cells")

# 1) Find header row per sheet (where first non-null col is "Years")
header_rows = (cells
    .filter(F.col("col_0") == "Years")
    .select("sheet_name", F.col("row_num").alias("header_row_num"))
)

# 2) Attach header_row_num back
cells_with_header = cells.join(header_rows, on="sheet_name", how="left")

# 3) For each sheet, find the indicator name from a couple of rows above the header
# In your file, this is typically row 2: e.g. 'Production of crude oil'
w = Window.partitionBy("sheet_name")
indicator_title_df = (
    cells_with_header
    .withColumn("indicator_title_row_num", F.col("header_row_num") - F.lit(2))
    .where(F.col("row_num") == F.col("indicator_title_row_num"))
    .select(
        "sheet_name",
        F.col("col_0").alias("indicator_name")
    )
)

cells_with_meta = cells_with_header.join(indicator_title_df, on="sheet_name", how="left")

# 4) Extract only the data rows: row_num > header_row_num
data_rows = cells_with_meta.filter(F.col("row_num") > F.col("header_row_num"))

# Now interpret:
# col_0 = Year, col_1 = Unit, col_2 = Value/Description
l0 = (
    data_rows
    .select(
        F.col("sheet_name").alias("indicator_code"),
        "indicator_name",
        F.col("col_0").alias("year_raw"),
        F.col("col_1").alias("unit"),
        F.col("col_2").alias("value_raw"),
        "file_name"
    )
)

# Basic cleaning: keep only numeric-like years, parse values
l0_clean = (
    l0
    .filter(F.col("year_raw").rlike("^[0-9]{4}$"))  # keep rows where year is YYYY
    .withColumn("year", F.col("year_raw").cast("int"))
    .withColumn(
        "value",
        F.regexp_replace(F.col("value_raw").cast("string"), ",", "").cast("double")
    )
    .withColumn("load_ts", F.current_timestamp())
    .drop("year_raw", "value_raw")
)

# Derive section from indicator_code (1.x = Production, 2.x = Consumption, 3.x = Trade)
l0_final = (
    l0_clean
    .withColumn("section",
                F.when(F.col("indicator_code").startswith("1."), F.lit("Production"))
                 .when(F.col("indicator_code").startswith("2."), F.lit("Consumption"))
                 .when(F.col("indicator_code").startswith("3."), F.lit("Trade"))
                 .otherwise(F.lit("Unknown")))
)

l0_final.write.format("delta").mode("overwrite").saveAsTable("l0_db.oil_gas_indicator_values")
