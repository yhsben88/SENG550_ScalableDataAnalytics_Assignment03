"""
spark_aggregate_incremental.py
Reads incremental raw folders (data/incremental/raw/{n}/orders_{n}.csv)
Updates processing/outputs/data/processed/orders.csv by appending/merging counts.
Uses redis to track max processed day.
Run: python spark_aggregate_incremental.py
"""

import os
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, expr, sum as _sum

INCR_RAW_GLOB_DIR = "data/incremental/raw"  # e.g., data/incremental/raw/0/...
PROCESSED_DIR = "processing/outputs/data/processed"  # same as full
REDIS_KEY = "orders:processed_max_dow"
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

def list_available_days():
    days = []
    if not os.path.exists(INCR_RAW_GLOB_DIR):
        return days
    for name in os.listdir(INCR_RAW_GLOB_DIR):
        if name.isdigit():
            days.append(int(name))
    return sorted(days)

def read_existing_agg(spark):
    path = os.path.join(PROCESSED_DIR, "orders.csv")
    if os.path.exists(path):
        # find the part file(s) inside the folder
        # spark read the CSV folder
        try:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            return df
        except Exception as e:
            print("No existing processed file or failed to read:", e)
            return None
    return None

def aggregate_from_folders(spark, days_to_process):
    # read all csv files in those folders
    paths = []
    for d in days_to_process:
        p = os.path.join(INCR_RAW_GLOB_DIR, str(d), f"orders_{d}.csv")
        if os.path.exists(p):
            paths.append(p)
    if not paths:
        return None
    df = spark.read.option("header","true").option("inferSchema","true").csv(paths)
    cols = df.columns
    exclude = set(["order_id", "order_dow", "order_hour_of_day", "user_id"])
    cat_cols = [c for c in cols if c not in exclude]
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
    agg = df2.groupBy("day_of_week", "hour_of_day", "category").agg(_sum("items_count").alias("items_count"))
    return agg

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    spark = SparkSession.builder.appName("orders_aggregate_incremental").getOrCreate()

    stored = r.get(REDIS_KEY)
    processed_max = int(stored.decode()) if stored else -1

    available = list_available_days()
    print("Available incremental folders:", available)
    days_to_process = [d for d in available if d > processed_max]
    if not days_to_process:
        print("Nothing new to process.")
        spark.stop()
        return

    print("Processing days:", days_to_process)
    new_agg = aggregate_from_folders(spark, days_to_process)
    if new_agg is None:
        print("No new data found.")
        spark.stop()
        return

    # read existing processed if any and merge (sum)
    existing = read_existing_agg(spark)
    if existing is not None:
        combined = existing.unionByName(new_agg, allowMissingColumns=True)
        final = combined.groupBy("day_of_week","hour_of_day","category").agg(_sum("items_count").alias("items_count"))
    else:
        final = new_agg

    out_path = os.path.join(PROCESSED_DIR, "orders.csv")
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    final.coalesce(1).write.option("header","true").mode("overwrite").csv(out_path)
    new_max = max(days_to_process)
    r.set(REDIS_KEY, new_max)
    print(f"Updated Redis {REDIS_KEY} -> {new_max}. Wrote processed CSV to {out_path}")
    spark.stop()

if __name__ == "__main__":
    main()
