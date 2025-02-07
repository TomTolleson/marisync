from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
import mlflow

def create_prediction_pipeline():
    spark = SparkSession.builder \
        .appName("Environmental ML Pipeline") \
        .getOrCreate()
        
    # Read processed data
    data = spark.read.format("delta").load("/data/processed/sensor_metrics")
    
    # Feature engineering
    feature_cols = ["avg_co2", "avg_temp", "avg_humidity", "max_pm25"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Create model
    gbt = GBTRegressor(labelCol="impact_score", featuresCol="features")
    
    # Build pipeline
    pipeline = Pipeline(stages=[assembler, gbt])
    
    return pipeline 