package connectors

import org.apache.spark.sql.DataFrame
import scala.util.Try

// Base trait for all ocean data connectors
trait OceanDataConnector {
  def fetchData(): Try[DataFrame]
  def validateData(df: DataFrame): Boolean
  def transformToCommonSchema(df: DataFrame): DataFrame
}

// Common configuration for API connectors
trait APIConfig {
  val apiKey: Option[String]
  val baseUrl: String
  val rateLimitPerMinute: Int
} 