package utils

import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.annotation.tailrec
import org.slf4j.LoggerFactory

trait RetryUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  def withRetry[T](operation: => T, config: RetryConfig = RetryConfig()): Try[T] = {
    @tailrec
    def retry(remainingAttempts: Int, delay: FiniteDuration): Try[T] = {
      val result = Try(operation)
      
      result match {
        case Success(value) => 
          Success(value)
          
        case Failure(exception) if remainingAttempts > 1 =>
          logger.warn(s"Operation failed, attempting retry in ${delay.toSeconds} seconds. Error: ${exception.getMessage}")
          Thread.sleep(delay.toMillis)
          val nextDelay = (delay * config.backoffFactor).toMillis.millis
          retry(
            remainingAttempts - 1,
            if (nextDelay > config.maxDelay) config.maxDelay else nextDelay
          )
          
        case Failure(exception) =>
          logger.error(s"Operation failed after ${config.maxAttempts} attempts. Final error: ${exception.getMessage}")
          Failure(exception)
      }
    }

    retry(config.maxAttempts, config.initialDelay)
  }
} 