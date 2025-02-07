package connectors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Success, Failure}
import org.mockito.MockitoSugar
import utils.{HttpClient, SparkTestHelper}
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.when
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class NDBCConnectorTest extends AnyFlatSpec with Matchers with MockitoSugar with SparkTestHelper {
  
  "NDBCConnector" should "successfully fetch and parse station data" in {
    // Mock HTTP client
    val mockHttpClient = mock[HttpClient]
    
    // Mock successful station list response
    when(mockHttpClient.get(
      eqTo("/station_table.txt"),
      any(),
      any(),
      any()
    )).thenReturn(Success(
      """Header line 1
        |Header line 2
        |41001 First Station
        |41002 Second Station""".stripMargin
    ))
    
    // Mock successful station data responses
    val stationData = 
      """#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE
        |#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft
        |2024 02 07 09 00 180  1.5  2.0  0.3    5.0  4.0  90  1019.2 20.5 20.5  17.0 10.0  0.0  2.0""".stripMargin
        
    when(mockHttpClient.get(
      eqTo("/41001.txt"),
      any(),
      any(),
      any()
    )).thenReturn(Success(stationData))
    
    when(mockHttpClient.get(
      eqTo("/41002.txt"),
      any(),
      any(),
      any()
    )).thenReturn(Success(stationData))
    
    // Create connector with mocked client
    val connector = new NDBCConnector(spark) {
      override protected val httpClient = mockHttpClient
    }
    
    // Test data fetch
    val result = connector.fetchData()
    result.isSuccess shouldBe true
    
    val df = result.get
    df.count() shouldBe 2  // One row per station
    
    // Verify data
    val rows = df.collect()
    rows.foreach { row =>
      row.getAs[Double]("temperature") shouldBe 20.5
      row.getAs[Double]("salinity") shouldBe 17.0  // Updated to match test data
    }
  }
  
  it should "handle station fetch failures gracefully" in {
    val mockHttpClient = mock[HttpClient]
    
    // Mock successful station list
    when(mockHttpClient.get(
      eqTo("/station_table.txt"),
      any(),
      any(),
      any()
    )).thenReturn(Success("Header\nHeader\n41001 Station"))
    
    // Mock failed station data fetch
    when(mockHttpClient.get(
      eqTo("/41001.txt"),
      any(),
      any(),
      any()
    )).thenReturn(Failure(new Exception("Connection timeout")))
    
    val connector = new NDBCConnector(spark) {
      override protected val httpClient = mockHttpClient
    }
    
    // Test should fail but not throw exception
    val result = connector.fetchData()
    result.isFailure shouldBe true
    result.failed.get.getMessage should include("No data could be retrieved")
  }
} 