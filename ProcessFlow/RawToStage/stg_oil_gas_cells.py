from pyspark.sql import functions as F, types as T

# --- Sample Path link provided for reference
raw_path = "abfss://data@account.dfs.core.windows.net/raw/oil_gas_statistics/2023/Oil_and_Gas_Statistics_2023_En.xlsx"

# You will need the spark-excel package, e.g.
# --packages com.crealytics:spark-excel_2.12:3.5.0_0.20.3

sheet_names = [  # could be config-driven
    "1.1","1.2","1.3","1.4","1.5","1.6","1.7","1.8","1.9",
    "2.1","2.2","2.3","2.4","2.5","2.6","2.7","2.8",
    "3.1","3.2","3.3","3.4","3.5","3.6","3.7","3.8","3.9","3.10","3.11","3.12","3.13"
]

staging_dfs = []

for sheet in sheet_names:
    df_sheet = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("dataAddress", f"'{sheet}'!A1")
        .option("header", "false")
        .option("inferSchema", "true")
        .load(raw_path)
        .withColumn("sheet_name", F.lit(sheet))
        .withColumn("file_name", F.lit("Oil_and_Gas_Statistics_2023_En.xlsx"))
    )
    staging_dfs.append(df_sheet)

stg_cells = staging_dfs[0]
for df in staging_dfs[1:]:
    stg_cells = stg_cells.unionByName(df, allowMissingColumns=True)

# Normalize column names as col_0, col_1, col_2, ...
renamed = stg_cells.select(
    *[
        F.col(c).alias(f"col_{i}") 
        for i, c in enumerate(stg_cells.columns) 
        if c not in ["sheet_name", "file_name"]
    ],
    "sheet_name",
    "file_name"
)

# Add row_num for later parsing
stg_cells_final = renamed.withColumn("row_num", F.monotonically_increasing_id())
stg_cells_final.write.format("delta").mode("overwrite").saveAsTable("stg_db.stg_oil_gas_cells")
