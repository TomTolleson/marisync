package pipeline

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SensorStreamProcessorTest extends AnyFlatSpec with Matchers {
  
  "SensorStreamProcessor" should "process sensor data correctly" in {
    val spark = SparkSession.builder()
      .appName("SensorStreamTest")
      .master("local[2]")
      // Add these configurations for Delta Lake
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Add Java 17 compatibility configs
      .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED")
      .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED")
      .getOrCreate()

    try {
      val processor = new SensorStreamProcessor(spark)
      
      // Start the stream processing with explicit Kafka config
      processor.processEnvironmentalStream("localhost:9092")
      
      // Let it run for a minute
      Thread.sleep(60000)
      
      // Verify the output
      val results = spark.read.format("delta")
        .load("data/processed/sensor_metrics")
      
      results.show()
      
      // Add assertions
      results.columns should contain allOf(
        "avg_co2", "avg_temp", "avg_humidity", "max_pm25"
      )
    } finally {
      spark.stop()
    }
  }
} 