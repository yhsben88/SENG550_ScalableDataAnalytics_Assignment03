"""
train.py
Train a Spark ML regression model to predict items_count given day_of_week, hour_of_day, category.
Saves model to processing/outputs/models/spark_regressor
"""
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline

PROCESSED_PATH = "processing/outputs/data/processed/orders.csv"
MODEL_DIR = "processing/outputs/models/spark_regressor"

def main():
    spark = SparkSession.builder.appName("orders_train").getOrCreate()
    df = spark.read.option("header","true").option("inferSchema","true").csv(PROCESSED_PATH)
    # columns: day_of_week, hour_of_day, category, items_count
    # Ensure types:
    df = df.withColumn("day_of_week", df["day_of_week"].cast("int")).withColumn("hour_of_day", df["hour_of_day"].cast("int")).withColumn("items_count", df["items_count"].cast("double"))
    si = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=["day_of_week", "hour_of_day", "category_idx"], outputCol="features", handleInvalid="keep")
    rf = RandomForestRegressor(featuresCol="features", labelCol="items_count", numTrees=20)
    pipeline = Pipeline(stages=[si, assembler, rf])
    model = pipeline.fit(df)
    os.makedirs(MODEL_DIR, exist_ok=True)
    model.write().overwrite().save(MODEL_DIR)
    print(f"Model saved to {MODEL_DIR}")
    spark.stop()

if __name__ == "__main__":
    main()
