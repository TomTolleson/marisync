package utils

import scalaj.http.{Http, HttpRequest, HttpResponse, HttpOptions}
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory
import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration._

object HttpClient {
  case class HttpConfig(
    rateLimitPerMinute: Int = 60,
    timeoutMs: Int = 10000,
    followRedirects: Boolean = true,
    retryConfig: RetryConfig = RetryConfig()
  )
}

class HttpClient(baseUrl: String) extends RetryUtils {
  import HttpClient._
  private val logger = LoggerFactory.getLogger(getClass)
  private val requestTimestamps = mutable.Queue[Instant]()
  
  def get(
    path: String,
    headers: Map[String, String] = Map.empty,
    params: Map[String, String] = Map.empty,
    config: HttpConfig = HttpConfig()
  ): Try[String] = {
    enforceRateLimit(config.rateLimitPerMinute)
    
    withRetry({
      val request = Http(s"$baseUrl$path")
        .headers(headers)
        .params(params)
        .timeout(
          connTimeoutMs = config.timeoutMs,
          readTimeoutMs = config.timeoutMs
        )
        .option(HttpOptions.followRedirects(config.followRedirects))
      
      val response = request.asString
      
      if (response.isSuccess) {
        Success(response.body)
      } else {
        Failure(new Exception(s"HTTP request failed with code ${response.code}: ${response.body}"))
      }
    }, config.retryConfig).flatten
  }

  private def enforceRateLimit(rateLimit: Int): Unit = {
    val now = Instant.now()
    val oneMinuteAgo = now.minusSeconds(60)
    
    // Remove requests older than 1 minute
    while (requestTimestamps.nonEmpty && requestTimestamps.head.isBefore(oneMinuteAgo)) {
      requestTimestamps.dequeue()
    }
    
    // If at rate limit, wait until we can make another request
    if (requestTimestamps.size >= rateLimit) {
      val oldestRequest = requestTimestamps.head
      val waitTime = 60 - (now.getEpochSecond - oldestRequest.getEpochSecond)
      if (waitTime > 0) {
        logger.info(s"Rate limit reached, waiting $waitTime seconds")
        Thread.sleep(waitTime * 1000)
      }
    }
    
    requestTimestamps.enqueue(now)
  }
} 