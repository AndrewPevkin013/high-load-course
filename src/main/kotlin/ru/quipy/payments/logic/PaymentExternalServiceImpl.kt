package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1),
        bucketSize = (rateLimitPerSec * 2).coerceAtLeast(1)
    )

    private val ongoingRequests = AtomicInteger(0)

    private fun remainingTime(deadline: Long): Long = deadline - now()

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        metricsReporter.recordPaymentStarted(accountName)
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        val startEs = System.currentTimeMillis()
        withContext(Dispatchers.IO) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }
        metricsReporter.recordEsUpdateDuration(System.currentTimeMillis() - startEs)

        logger.debug("[$accountName] Submit: $paymentId, txId: $transactionId")

        executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, 1)
    }

    private suspend fun executePaymentWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        if (remainingTime(deadline) <= 0) {
            logger.warn("[$accountName] Deadline exceeded for payment $paymentId")
            metricsReporter.recordDeadlineExceeded(accountName)
            metricsReporter.incrementRetry(accountName, RetryCause.DEADLINE)
            withContext(Dispatchers.IO) {
                val startEs = System.currentTimeMillis()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                }
                metricsReporter.recordEsUpdateDuration(System.currentTimeMillis() - startEs)
            }
            return
        }

        // Rate limiter
        val rateAllowed = rateLimiter.tick()
        metricsReporter.recordRateLimiterResult(accountName, rateAllowed)
        if (!rateAllowed) {
            logger.warn("[$accountName] Rate limit exceeded for payment $paymentId")
            metricsReporter.incrementRetry(accountName, RetryCause.RATE_LIMITER)
            withContext(Dispatchers.IO) {
                val startEs = System.currentTimeMillis()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Rate limit exceeded")
                }
                metricsReporter.recordEsUpdateDuration(System.currentTimeMillis() - startEs)
            }
            return
        }

        if (ongoingRequests.incrementAndGet() > parallelRequests) {
            ongoingRequests.decrementAndGet()
            logger.warn("[$accountName] Too many parallel requests for payment $paymentId")
            metricsReporter.recordParallelLimitBlocked(accountName)

            if (attempt < 3 && remainingTime(deadline) > 500) {
                delay(calculateBackoff(attempt))
                executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
            } else {
                metricsReporter.incrementRetry(accountName, RetryCause.PARALLEL_LIMIT)
                withContext(Dispatchers.IO) {
                    val startEs = System.currentTimeMillis()
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Too many parallel requests")
                    }
                    metricsReporter.recordEsUpdateDuration(System.currentTimeMillis() - startEs)
                }
            }
            return
        }

        try {
            val timeout = remainingTime(deadline).coerceIn(1, 30000)
            metricsReporter.updateCurrentTimeout(accountName, timeout)

            val url = "http://$paymentProviderHostPort/external/process?" +
                    "serviceName=$serviceName&token=$token&accountName=$accountName&" +
                    "transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

            val request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeout))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            val httpStart = System.currentTimeMillis()
            val response = try {
                withTimeout(timeout) {
                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                }
            } catch (e: TimeoutCancellationException) {
                metricsReporter.recordHttpResponse(accountName, -1)
                metricsReporter.recordHttpRequestDuration(accountName, System.currentTimeMillis() - httpStart)
                metricsReporter.incrementRetry(accountName, RetryCause.TIMEOUT)
                logger.error("[$accountName] HTTP timeout for payment $paymentId")
                throw e
            }
            val httpDuration = System.currentTimeMillis() - httpStart
            metricsReporter.recordHttpRequestDuration(accountName, httpDuration)
            metricsReporter.recordHttpResponse(accountName, response.statusCode())

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] Parse error for payment $paymentId: ${response.body()}")
                metricsReporter.recordParseError(accountName)
                metricsReporter.incrementRetry(accountName, RetryCause.PARSE_ERROR)
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.debug("[$accountName] Payment result for txId: $transactionId, succeeded: ${body.result}")

            val startEs = System.currentTimeMillis()
            withContext(Dispatchers.IO) {
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
            metricsReporter.recordEsUpdateDuration(System.currentTimeMillis() - startEs)

            if (!body.result && attempt < 3 && remainingTime(deadline) > 500) {
                val cause = when (response.statusCode()) {
                    429 -> RetryCause.HTTP_429
                    in 400..499 -> RetryCause.HTTP_4XX
                    in 500..599 -> RetryCause.HTTP_5XX
                    else -> RetryCause.UNKNOWN
                }
                metricsReporter.incrementRetry(accountName, cause)
                delay(calculateBackoff(attempt))
                executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Error processing payment $paymentId (attempt $attempt)", e)
            val cause = when (e) {
                is TimeoutCancellationException -> RetryCause.TIMEOUT
                is java.net.http.HttpTimeoutException -> RetryCause.TIMEOUT
                is java.net.ConnectException -> RetryCause.NETWORK_ERROR
                else -> RetryCause.UNKNOWN
            }
            metricsReporter.incrementRetry(accountName, cause)

            if (attempt < 3 && remainingTime(deadline) > 500) {
                delay(calculateBackoff(attempt))
                executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
            } else {
                val startEs = System.currentTimeMillis()
                withContext(Dispatchers.IO) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Failed after $attempt attempts: ${e.message}")
                    }
                }
                metricsReporter.recordEsUpdateDuration(System.currentTimeMillis() - startEs)
            }
        } finally {
            ongoingRequests.decrementAndGet()
        }
    }

    private fun calculateBackoff(attempt: Int): Long {
        return when (attempt) {
            1 -> 100L
            2 -> 200L
            else -> 400L
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()