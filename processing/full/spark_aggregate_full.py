"""
spark_aggregate_full.py
Run with: spark-submit spark_aggregate_full.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, lit, sum as _sum
from pyspark.sql.functions import expr

RAW_GLOB = "../../part1_data_split/part1/data/raw/*/orders_*.csv"
OUT_DIR = "processing/outputs/data/processed"

def main():
    spark = SparkSession.builder.appName("orders_aggregate_full").getOrCreate()
    # read all raw files (they have headers)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_GLOB)
    # Expect columns: order_id, order_dow (day_of_week), order_hour_of_day, plus category columns with counts
    cols = df.columns
    # Identify category columns: exclude common columns
    exclude = set(["order_id", "order_dow", "order_hour_of_day", "user_id"])
    cat_cols = [c for c in cols if c not in exclude]
    if not cat_cols:
        print("Warning: No category columns detected - using all columns except order_id, order_dow, order_hour_of_day.")
        cat_cols = [c for c in cols if c not in exclude]

    # Create (category, count) structs per row and explode
    # Build an expression that creates an array of structs: named (category, cnt)
    arr_expr = ", ".join([f"named_struct('category','{c}','cnt',`{c}`)" for c in cat_cols])
    df2 = df.select(
        col("order_dow").alias("day_of_week"),
        col("order_hour_of_day").alias("hour_of_day"),
        expr(f"array({arr_expr}) as cats")
    ).withColumn("cat", explode(col("cats"))).select(
        col("day_of_week"),
        col("hour_of_day"),
        col("cat.category").alias("category"),
        col("cat.cnt").cast("long").alias("items_count")
    )

    # Some rows may have zeros; sum per (day_of_week, hour_of_day, category)
    agg = df2.groupBy("day_of_week", "hour_of_day", "category").agg(_sum("items_count").alias("items_count"))
    os.makedirs(OUT_DIR, exist_ok=True)
    # write as CSV (multiple files ok)
    agg.coalesce(1).write.option("header", "true").mode("overwrite").csv(os.path.join(OUT_DIR, "orders.csv"))
    print(f"Wrote aggregated output to {OUT_DIR}/orders.csv")
    spark.stop()

if __name__ == "__main__":
    main()
