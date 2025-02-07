package utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkTestHelper extends BeforeAndAfterAll { self: Suite =>
  
  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .appName("Test")
      .master("local[2]")
      // Disable Spark UI to avoid json4s dependency issues
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
  }
} 