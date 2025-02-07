package utils

import scala.concurrent.duration._

case class RetryConfig(
  maxAttempts: Int = 3,
  initialDelay: FiniteDuration = 1.second,
  maxDelay: FiniteDuration = 1.minute,
  backoffFactor: Double = 2.0
) 