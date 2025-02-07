package models

import java.time.Instant

case class SensorReading(
  timestamp: String,
  location: String,
  depth: Double,
  temperature: Double,
  salinity: Double,
  dissolved_oxygen: Option[Double],
  ph: Option[Double],
  turbidity: Option[Double],
  chlorophyll: Option[Double],
  current_speed: Option[Double],
  current_direction: Option[Double]
)

// Companion object for helper methods
object SensorReading {
  def fromJson(json: Map[String, Any]): SensorReading = {
    SensorReading(
      timestamp = json("timestamp").toString,
      location = json("location").toString,
      depth = json.getOrElse("depth", 0.0).toString.toDouble,
      temperature = json("temperature").toString.toDouble,
      salinity = json.getOrElse("salinity", 0.0).toString.toDouble,
      dissolved_oxygen = json.get("dissolved_oxygen").map(_.toString.toDouble),
      ph = json.get("ph").map(_.toString.toDouble),
      turbidity = json.get("turbidity").map(_.toString.toDouble),
      chlorophyll = json.get("chlorophyll").map(_.toString.toDouble),
      current_speed = json.get("current_speed").map(_.toString.toDouble),
      current_direction = json.get("current_direction").map(_.toString.toDouble)
    )
  }
} 