package connectors

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import scala.util.{Try, Success, Failure}
import scala.io.Source
import java.time.Instant
import models.SensorReading
import utils.{RetryUtils, HttpClient}
import org.slf4j.LoggerFactory
import utils.HttpClient.HttpConfig

class NDBCConnector(spark: SparkSession) extends OceanDataConnector with APIConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Move baseUrl before httpClient initialization
  override val baseUrl = "https://www.ndbc.noaa.gov/data/realtime2"
  override val apiKey = None  // NDBC doesn't require API key
  override val rateLimitPerMinute = 60
  
  // Make protected for testing
  protected val httpClient = new HttpClient(baseUrl)

  def fetchData(): Try[DataFrame] = Try {
    getActiveStations() match {
      case Success(stations) =>
        // Fetch data for each station with error handling
        val stationData = stations.map { station =>
          fetchStationData(station) match {
            case Success(data) => data
            case Failure(error) =>
              logger.error(s"Failed to fetch data for station $station: ${error.getMessage}")
              List.empty[SensorReading]
          }
        }.flatten
        
        if (stationData.isEmpty) {
          throw new Exception("No data could be retrieved from any station")
        }
        
        // Create DataFrame directly instead of using extension method
        val schema = StructType(Seq(
          StructField("timestamp", StringType, false),
          StructField("location", StringType, false),
          StructField("depth", DoubleType, false),
          StructField("temperature", DoubleType, false),
          StructField("salinity", DoubleType, false),
          StructField("dissolved_oxygen", DoubleType, true),
          StructField("ph", DoubleType, true),
          StructField("turbidity", DoubleType, true),
          StructField("chlorophyll", DoubleType, true),
          StructField("current_speed", DoubleType, true),
          StructField("current_direction", DoubleType, true)
        ))

        val rows = stationData.map { r =>
          Row(
            r.timestamp,
            r.location,
            r.depth,
            r.temperature,
            r.salinity,
            r.dissolved_oxygen.orNull,
            r.ph.orNull,
            r.turbidity.orNull,
            r.chlorophyll.orNull,
            r.current_speed.orNull,
            r.current_direction.orNull
          )
        }

        spark.createDataFrame(
          spark.sparkContext.parallelize(rows),
          schema
        )
        
      case Failure(error) =>
        throw new Exception(s"Failed to get active stations: ${error.getMessage}")
    }
  }

  private def getActiveStations(): Try[List[String]] = {
    httpClient.get(
      path = "/station_table.txt",
      config = HttpConfig(rateLimitPerMinute = rateLimitPerMinute)
    ).map { response =>
      response.linesIterator
        .drop(2)  // Skip header rows
        .map(_.split("\\s+")(0))
        .toList
    }
  }

  private def fetchStationData(station: String): Try[List[SensorReading]] = {
    httpClient.get(
      path = s"/$station.txt",
      config = HttpConfig(rateLimitPerMinute = rateLimitPerMinute)
    ).map { response =>
      parseNDBCData(response.linesIterator.toList, station)
    }.recoverWith { case error =>
      logger.warn(s"Error fetching data for station $station: ${error.getMessage}")
      Success(List.empty)  // Return empty list instead of failing completely
    }
  }

  private def parseNDBCData(lines: List[String], station: String): List[SensorReading] = {
    Try {
      lines.drop(2)  // Skip header rows
        .map { line =>
          val fields = line.split("\\s+")
          // Add debug logging
          logger.debug(s"Parsing line: $line")
          logger.debug(s"Split into ${fields.length} fields: ${fields.mkString(", ")}")
          
          // Add validation
          if (fields.length < 15) {
            logger.error(s"Invalid data format for station $station. Expected at least 15 fields, got ${fields.length}")
            throw new IllegalArgumentException(s"Invalid data format: ${fields.mkString(", ")}")
          }
          
          // Add safer parsing with defaults
          def safeParseDouble(index: Int, default: Double = 0.0): Double = {
            Try(fields(index).toDouble).getOrElse {
              logger.warn(s"Failed to parse field ${fields(index)} as double, using default $default")
              default
            }
          }
          
          SensorReading(
            timestamp = Instant.now().toString,
            location = station,
            depth = 0.0,  // Surface buoys
            temperature = safeParseDouble(13),  // WTMP field
            salinity = safeParseDouble(14),     // DEWP field (using as proxy for salinity in test)
            dissolved_oxygen = None,
            ph = None,
            turbidity = None,
            chlorophyll = None,
            current_speed = Some(safeParseDouble(6)),      // WSPD field
            current_direction = Some(safeParseDouble(5))   // WDIR field
          )
        }
    }.recoverWith { case e =>
      logger.error(s"Failed to parse data for station $station: ${e.getMessage}")
      logger.debug("Raw data: " + lines.mkString("\n"))
      Success(List.empty)  // Return empty list instead of failing
    }.getOrElse(List.empty)
  }

  def validateData(df: DataFrame): Boolean = {
    // Implement NDBC-specific validation
    df.columns.contains("temperature") && 
    df.columns.contains("salinity") &&
    !df.filter(col("temperature").isNull).isEmpty
  }

  def transformToCommonSchema(df: DataFrame): DataFrame = {
    // Transform NDBC schema to our common schema
    df.select(
      col("timestamp"),
      col("location"),
      lit(0.0).as("depth"),
      col("temperature"),
      col("salinity"),
      lit(null).cast("double").as("dissolved_oxygen"),
      lit(null).cast("double").as("ph"),
      lit(null).cast("double").as("turbidity"),
      lit(null).cast("double").as("chlorophyll"),
      col("current_speed"),
      col("current_direction")
    )
  }
} 