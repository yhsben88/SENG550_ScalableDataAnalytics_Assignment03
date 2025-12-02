"""
push_predictions_to_redis.py
Loads model, generates predictions for all combinations of (day_of_week 0..6, hour_of_day 0..23, categories),
and pushes to Redis with key format day:hour:category -> predicted_value
"""
import os
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

MODEL_DIR = "processing/outputs/models/spark_regressor"
REDIS_HOST = os.environ.get("REDIS_HOST","localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT",6379))

def main():
    spark = SparkSession.builder.appName("push_predictions").getOrCreate()
    from pyspark.ml import PipelineModel
    model = PipelineModel.load(MODEL_DIR)
    # obtain list of categories from processed data
    processed = spark.read.option("header","true").option("inferSchema","true").csv("processing/outputs/data/processed/orders.csv")
    categories = [r['category'] for r in processed.select("category").distinct().collect()]
    rows = []
    for dow in range(7):
        for hr in range(24):
            for cat in categories:
                rows.append((dow, hr, cat))
    df = spark.createDataFrame(rows, schema=["day_of_week","hour_of_day","category"])
    preds = model.transform(df).select("day_of_week","hour_of_day","category","prediction")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    for row in preds.collect():
        key = f"{int(row['day_of_week'])}:{int(row['hour_of_day'])}:{row['category']}"
        value = float(row['prediction'])
        r.set(key, value)
    print(f"Pushed {preds.count()} predictions to Redis at {REDIS_HOST}:{REDIS_PORT}")
    spark.stop()

if __name__ == "__main__":
    main()
