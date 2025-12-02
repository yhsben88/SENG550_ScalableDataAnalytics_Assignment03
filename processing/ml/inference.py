"""
inference.py
Load trained model, accept day_of_week, hour_of_day, category (via args or interactive), and output predicted items_count.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

MODEL_DIR = "processing/outputs/models/spark_regressor"

def predict(day_of_week, hour_of_day, category):
    spark = SparkSession.builder.appName("orders_inference").getOrCreate()
    model = spark.read.format("parquet").load(MODEL_DIR) if False else None
    from pyspark.ml import PipelineModel
    model = PipelineModel.load(MODEL_DIR)
    r = spark.createDataFrame([Row(day_of_week=int(day_of_week), hour_of_day=int(hour_of_day), category=str(category))])
    pred = model.transform(r).select("prediction").collect()[0][0]
    print(f"Predicted items_count: {pred}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python inference.py <day_of_week> <hour_of_day> <category>")
        sys.exit(1)
    predict(sys.argv[1], sys.argv[2], sys.argv[3])
