package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{window, col, from_json}
import java.nio.file.Paths
import org.apache.spark.sql.expressions.Window

class SensorStreamProcessor(spark: SparkSession) {
  import spark.implicits._
  
  // Quality control thresholds
  private object QualityThresholds {
    // Temperature bounds (°C) by depth layer
    val TempBounds = Map(
      "surface" -> (-2.0, 35.0),
      "mixed_layer" -> (-2.0, 30.0),
      "mesopelagic" -> (-2.0, 25.0),
      "deep_ocean" -> (-2.0, 20.0)
    )
    
    // Salinity bounds (PSU)
    val SalinityBounds = (28.0, 41.0)
    
    // Dissolved oxygen bounds (mg/L) by depth layer
    val OxygenBounds = Map(
      "surface" -> (4.0, 12.0),
      "mixed_layer" -> (2.0, 10.0),
      "mesopelagic" -> (0.5, 8.0),
      "deep_ocean" -> (0.1, 6.0)
    )
    
    // pH bounds
    val pHBounds = (7.0, 8.5)
    
    // Turbidity bounds (NTU)
    val TurbidityBounds = (0.0, 50.0)
    
    // Chlorophyll bounds (μg/L) by depth layer
    val ChlorophyllBounds = Map(
      "surface" -> (0.0, 50.0),
      "mixed_layer" -> (0.0, 20.0),
      "mesopelagic" -> (0.0, 5.0),
      "deep_ocean" -> (0.0, 1.0)
    )
    
    // Current speed bounds (m/s)
    val CurrentSpeedBounds = (0.0, 3.0)
    
    // Rate of change thresholds (per meter)
    val MaxTempChange = 0.5  // °C/m
    val MaxSalinityChange = 0.2  // PSU/m
    val MaxOxygenChange = 0.5  // mg/L/m
  }

  // Define schema for oceanic sensor data
  private val sensorSchema = StructType(Seq(
    StructField("timestamp", StringType, false),
    StructField("location", StringType, false),
    StructField("depth", DoubleType, false),
    StructField("temperature", DoubleType, false),
    StructField("salinity", DoubleType, false),
    StructField("dissolved_oxygen", DoubleType, false),
    StructField("ph", DoubleType, false),
    StructField("turbidity", DoubleType, false),
    StructField("chlorophyll", DoubleType, true),  // Optional field for phytoplankton activity
    StructField("current_speed", DoubleType, true), // Optional field for water movement
    StructField("current_direction", DoubleType, true)
  ))

  def processEnvironmentalStream(kafkaBootstrapServers: String = "localhost:9092"): Unit = {
    // Get current working directory
    val projectDir = System.getProperty("user.dir")
    val checkpointLocation = Paths.get(projectDir, "data/checkpoint/sensors").toString
    val outputLocation = Paths.get(projectDir, "data/processed/sensor_metrics").toString
    
    val sensorDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", "environmental-sensors")
      .option("startingOffsets", "earliest")
      .load()
      
    // Parse JSON data from Kafka
    val parsedDF = sensorDF
      .select(
        from_json(col("value").cast("string"), sensorSchema).as("data")
      )
      .select("data.*")
      .withColumn("timestamp", to_timestamp(col("timestamp")))
      
    // First, calculate depth layers for better vertical profiling
    val parsedWithLayers = parsedDF
      .withColumn("depth_layer", 
        when(col("depth") <= 10, "surface")
        .when(col("depth") <= 100, "mixed_layer")
        .when(col("depth") <= 1000, "mesopelagic")
        .otherwise("deep_ocean")
      )
      
    // Calculate density using UNESCO formula (simplified)
    .withColumn("density", 
      // Simplified density calculation based on temperature and salinity
      expr("1000 + (0.8 * salinity) - (0.2 * temperature)")
    )
    
    // Calculate derived metrics
    .withColumn("oxygen_saturation",
      // Simplified oxygen saturation calculation
      expr("dissolved_oxygen / (14.6 - 0.4 * temperature)")
    )

    // Add quality control checks
    val qualityCheckedDF = parsedWithLayers
      // Temperature QC
      .withColumn("temp_qc_flag",
        when(
          col("temperature").between(
            expr(s"element_at(array(${QualityThresholds.TempBounds.map { case (k, (min, max)) => 
              s"when(depth_layer = '$k', $min)"}.mkString(", ")}), 1)"),
            expr(s"element_at(array(${QualityThresholds.TempBounds.map { case (k, (min, max)) => 
              s"when(depth_layer = '$k', $max)"}.mkString(", ")}), 1)")
          ), "pass"
        ).otherwise("fail")
      )
      // Salinity QC
      .withColumn("salinity_qc_flag",
        when(col("salinity").between(
          lit(QualityThresholds.SalinityBounds._1),
          lit(QualityThresholds.SalinityBounds._2)
        ), "pass").otherwise("fail")
      )
      // Oxygen QC
      .withColumn("oxygen_qc_flag",
        when(
          col("dissolved_oxygen").between(
            expr(s"element_at(array(${QualityThresholds.OxygenBounds.map { case (k, (min, max)) => 
              s"when(depth_layer = '$k', $min)"}.mkString(", ")}), 1)"),
            expr(s"element_at(array(${QualityThresholds.OxygenBounds.map { case (k, (min, max)) => 
              s"when(depth_layer = '$k', $max)"}.mkString(", ")}), 1)")
          ), "pass"
        ).otherwise("fail")
      )
      // pH QC
      .withColumn("ph_qc_flag",
        when(col("ph").between(
          lit(QualityThresholds.pHBounds._1),
          lit(QualityThresholds.pHBounds._2)
        ), "pass").otherwise("fail")
      )
      // Rate of change QC (using window functions)
      .withColumn("depth_ordered",
        row_number().over(Window.partitionBy("location").orderBy("depth")))
      .withColumn("next_depth",
        lead("depth", 1).over(Window.partitionBy("location").orderBy("depth")))
      .withColumn("next_temp",
        lead("temperature", 1).over(Window.partitionBy("location").orderBy("depth")))
      .withColumn("temp_gradient_qc",
        when(abs(col("next_temp") - col("temperature")) / 
             (col("next_depth") - col("depth")) > QualityThresholds.MaxTempChange, "fail")
          .otherwise("pass"))

    val processedData = qualityCheckedDF
      .withWatermark("timestamp", "15 minutes")
      // Group by time window, location, and depth layer for better vertical resolution
      .groupBy(
        window(col("timestamp"), "1 hour"),  // Increased window for oceanic timescales
        col("location"),
        col("depth_layer")
      )
      .agg(
        // Temperature statistics
        avg("temperature").as("temp_avg"),
        stddev("temperature").as("temp_std"),
        min("temperature").as("temp_min"),
        max("temperature").as("temp_max"),
        
        // Salinity statistics with density considerations
        avg("salinity").as("salinity_avg"),
        stddev("salinity").as("salinity_std"),
        avg("density").as("density_avg"),
        
        // Oxygen metrics with saturation
        avg("dissolved_oxygen").as("oxygen_avg"),
        avg("oxygen_saturation").as("oxygen_saturation_avg"),
        min("dissolved_oxygen").as("oxygen_min"),
        
        // pH statistics with range detection
        avg("ph").as("ph_avg"),
        max(abs(col("ph") - avg("ph").over())).as("ph_max_deviation"),
        
        // Turbidity and chlorophyll (important for biological activity)
        avg("turbidity").as("turbidity_avg"),
        max("turbidity").as("turbidity_max"),
        avg("chlorophyll").as("chlorophyll_avg"),
        max("chlorophyll").as("chlorophyll_max"),
        
        // Current analysis
        avg("current_speed").as("current_speed_avg"),
        max("current_speed").as("current_speed_max"),
        // Circular mean for direction
        avg(expr("sin(radians(current_direction))")).as("current_direction_sin_avg"),
        avg(expr("cos(radians(current_direction))")).as("current_direction_cos_avg"),
        
        // Metadata
        count("*").as("measurement_count"),
        min("depth").as("min_depth"),
        max("depth").as("max_depth"),
        
        // Add QC summary metrics
        sum(when(col("temp_qc_flag") === "fail", 1).otherwise(0)).as("temp_qc_failures"),
        sum(when(col("salinity_qc_flag") === "fail", 1).otherwise(0)).as("salinity_qc_failures"),
        sum(when(col("oxygen_qc_flag") === "fail", 1).otherwise(0)).as("oxygen_qc_failures"),
        sum(when(col("ph_qc_flag") === "fail", 1).otherwise(0)).as("ph_qc_failures"),
        sum(when(col("temp_gradient_qc") === "fail", 1).otherwise(0)).as("gradient_qc_failures")
      )
      
    // Add derived calculations
    val enrichedData = processedData
      // Calculate true current direction from sin/cos averages
      .withColumn("current_direction_avg", 
        expr("degrees(atan2(current_direction_sin_avg, current_direction_cos_avg))")
      )
      // Calculate mixed layer indicators
      .withColumn("temp_gradient", 
        expr("(temp_max - temp_min) / (max_depth - min_depth)")
      )
      // Add quality metrics
      .withColumn("data_quality", 
        when(col("measurement_count") >= 10, "high")
        .when(col("measurement_count") >= 5, "medium")
        .otherwise("low")
      )
      
      // Add overall quality score
      .withColumn("qc_score",
        (lit(100) - 
         (col("temp_qc_failures") + 
          col("salinity_qc_failures") + 
          col("oxygen_qc_failures") + 
          col("ph_qc_failures") +
          col("gradient_qc_failures")) * lit(5))
      )
      .withColumn("quality_category",
        when(col("qc_score") >= 95, "excellent")
          .when(col("qc_score") >= 85, "good")
          .when(col("qc_score") >= 75, "fair")
          .otherwise("poor")
      )
    
    // Write to Delta table
    enrichedData.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .start(outputLocation)
  }
} 