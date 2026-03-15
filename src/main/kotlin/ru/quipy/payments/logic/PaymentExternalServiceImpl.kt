package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.math.pow

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val dbScope: CoroutineScope
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val requestAverageProcessingTime = properties.averageProcessingTime

    private val client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(50))
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofMillis(50))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)

    private val retryCount = 2
    private val baseDelay = requestAverageProcessingTime.toMillis().coerceAtLeast(10)
    private val maxDelay = 50L

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.info("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val submittedAt = now()

        withContext(dbScope.coroutineContext) {
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = true,
                    transactionId = transactionId,
                    startedAt = submittedAt,
                    spentInQueueDuration = Duration.ofMillis(submittedAt - paymentStartedAt)
                )
            }
        }

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        executePaymentWithRetry(paymentId, amount, transactionId, deadline)
    }

    private suspend fun executePaymentWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ) {
        var attempt = 0
        var processed = false

        while (!processed && attempt < retryCount && now() < deadline) {
            attempt++

            if (!waitForRateLimit(deadline)) {
                logger.error("[$accountName] Rate limit timeout for payment $paymentId")
                logProcessing(paymentId, transactionId, false, "Rate limit timeout")
                return
            }

            val remainingToAcquire = deadline - now()
            if (remainingToAcquire <= 0) {
                break
            }

            val acquired = withTimeoutOrNull(remainingToAcquire) {
                semaphore.acquire()
                true
            } ?: false

            if (!acquired) {
                logger.error("[$accountName] Semaphore timeout for payment $paymentId")
                logProcessing(paymentId, transactionId, false, "Semaphore timeout")
                return
            }

            try {
                val response = executeHttpRequest(paymentId, amount, transactionId, deadline)

                if (response == null) {
                    logger.error("[$accountName] Payment timeout for payment $paymentId (attempt $attempt)")

                    if (attempt >= retryCount || now() >= deadline) {
                        logProcessing(paymentId, transactionId, false, "Request timeout")
                        processed = true
                    } else {
                        delay(exponentialBackoffDelay(attempt))
                    }

                    continue
                }

                logger.info("[$accountName] Payment result for txId: $transactionId, succeeded: ${response.result}")

                if (response.result) {
                    logProcessing(paymentId, transactionId, true, response.message)
                    processed = true
                } else if (response.message == "Temporary error" && attempt < retryCount && now() < deadline) {
                    delay(exponentialBackoffDelay(attempt))
                } else {
                    logProcessing(paymentId, transactionId, false, response.message)
                    processed = true
                }

            } catch (e: Exception) {
                when (e.cause) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Socket timeout for payment $paymentId", e)
                    }
                    else -> {
                        logger.error("[$accountName] Error processing payment $paymentId (attempt $attempt)", e)
                    }
                }

                if (attempt < retryCount && now() < deadline) {
                    delay(exponentialBackoffDelay(attempt))
                } else {
                    logProcessing(
                        paymentId,
                        transactionId,
                        false,
                        "Failed after $attempt attempts: ${e.message}"
                    )
                    processed = true
                }
            } finally {
                semaphore.release()
            }
        }

        if (!processed) {
            logger.error("[$accountName] Deadline exceeded for payment $paymentId")
            logProcessing(paymentId, transactionId, false, "Deadline exceeded")
        }
    }

    private suspend fun waitForRateLimit(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (now() >= deadline) {
                return false
            }
            delay(2)
        }
        return true
    }

    private suspend fun executeHttpRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): ExternalSysResponse? {
        val remainingTime = deadline - now()
        if (remainingTime <= 50) {
            return null
        }

        val url =
            "http://$paymentProviderHostPort/external/process" +
                    "?serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val response = withTimeoutOrNull(remainingTime) {
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
        } ?: return null

        return try {
            mapper.readValue(response.body(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] Parse error for payment $paymentId: ${response.body()}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }
    }

    private suspend fun logProcessing(
        paymentId: UUID,
        transactionId: UUID,
        success: Boolean,
        reason: String?
    ) {
        withContext(dbScope.coroutineContext) {
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = reason)
            }
        }
    }

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf(
            (baseDelay * 2.0.pow((attempt - 1).toDouble())).toLong(),
            maxDelay
        )
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()