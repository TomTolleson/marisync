package connectors

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scala.util.Try
import org.json4s._
import org.json4s.native.JsonMethods._
import scalaj.http.{Http, HttpOptions}
import models.SensorReading

class OOIConnector(spark: SparkSession, apiKeyValue: String) extends OceanDataConnector with APIConfig {
  val baseUrl = "https://ooinet.oceanobservatories.org/api/m2m"
  val apiKey = Some(apiKeyValue)
  val rateLimitPerMinute = 100

  def fetchData(): Try[DataFrame] = Try {
    import spark.implicits._
    
    // Get list of available instruments
    val instruments = getInstruments()
    
    // Fetch data for each instrument
    instruments.flatMap { case (node, instrument) =>
      fetchInstrumentData(node, instrument)
    }.toDF()
  }

  private def getInstruments(): List[(String, String)] = {
    val response = Http(s"$baseUrl/12587/sensor/inv")
      .header("Authorization", apiKey.get)
      .option(HttpOptions.followRedirects(true))
      .asString

    implicit val formats = DefaultFormats
    parse(response.body).extract[List[(String, String)]]
  }

  private def fetchInstrumentData(node: String, instrument: String): List[SensorReading] = {
    val response = Http(s"$baseUrl/12587/sensor/inv/$node/$instrument")
      .header("Authorization", apiKey.get)
      .param("beginDT", getStartTime())
      .param("endDT", getEndTime())
      .option(HttpOptions.followRedirects(true))
      .asString

    parseOOIData(response.body)
  }

  private def parseOOIData(jsonData: String): List[SensorReading] = {
    implicit val formats = DefaultFormats
    val data = parse(jsonData)
    
    // Extract data points from OOI JSON format
    (data \ "data").extract[List[Map[String, Any]]].map { point =>
      SensorReading(
        timestamp = (point("time").toString),
        location = point("platform_code").toString,
        depth = point.getOrElse("depth", 0.0).toString.toDouble,
        temperature = point.getOrElse("temperature", 0.0).toString.toDouble,
        salinity = point.getOrElse("salinity", 0.0).toString.toDouble,
        dissolved_oxygen = Some(point.getOrElse("dissolved_oxygen", 0.0).toString.toDouble),
        ph = Some(point.getOrElse("ph", 0.0).toString.toDouble),
        turbidity = Some(point.getOrElse("turbidity", 0.0).toString.toDouble),
        chlorophyll = Some(point.getOrElse("fluorescence", 0.0).toString.toDouble),
        current_speed = Some(point.getOrElse("current_speed", 0.0).toString.toDouble),
        current_direction = Some(point.getOrElse("current_direction", 0.0).toString.toDouble)
      )
    }
  }

  private def getStartTime(): String = {
    // Return ISO8601 formatted time 24 hours ago
    java.time.Instant.now().minusSeconds(86400).toString
  }

  private def getEndTime(): String = {
    // Return current time in ISO8601 format
    java.time.Instant.now().toString
  }

  def validateData(df: DataFrame): Boolean = {
    // OOI-specific validation
    df.columns.contains("temperature") &&
    df.columns.contains("depth") &&
    !df.filter(col("temperature").isNull).isEmpty
  }

  def transformToCommonSchema(df: DataFrame): DataFrame = {
    // Transform OOI schema to common schema
    df.select(
      col("timestamp"),
      col("location"),
      col("depth"),
      col("temperature"),
      col("salinity"),
      col("dissolved_oxygen"),
      col("ph"),
      col("turbidity"),
      col("chlorophyll"),
      col("current_speed"),
      col("current_direction")
    )
  }
} 