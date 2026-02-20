package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*

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
        .executor(Executors.newFixedThreadPool(parallelRequests))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val semaphore = Semaphore(parallelRequests)

    private val coroutineDispatcher = Executors.newFixedThreadPool(parallelRequests).asCoroutineDispatcher()
    private val coroutineScope = CoroutineScope(coroutineDispatcher + SupervisorJob())

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        withContext(Dispatchers.IO) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.debug("[$accountName] Submit: $paymentId, txId: $transactionId")

        coroutineScope.launch {
            executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, 1)
        }
    }

    private suspend fun executePaymentWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        if (now() > deadline) {
            logger.warn("[$accountName] Deadline exceeded for payment $paymentId")
            withContext(Dispatchers.IO) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                }
            }
            return
        }

        try {
            if (!waitForRateLimitAsync(deadline)) {
                logger.warn("[$accountName] Rate limit timeout for payment $paymentId")
                withContext(Dispatchers.IO) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Rate limit timeout")
                    }
                }
                return
            }

            val acquired = try {
                withTimeout(deadline - now()) {
                    semaphore.acquire()
                    true
                }
            } catch (e: TimeoutCancellationException) {
                false
            }

            if (!acquired) {
                logger.warn("[$accountName] Semaphore timeout for payment $paymentId")
                withContext(Dispatchers.IO) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout")
                    }
                }
                return
            }

            try {
                executeHttpRequestAsync(paymentId, amount, transactionId)
            } finally {
                semaphore.release()
            }

        } catch (e: Exception) {
            logger.error("[$accountName] Error processing payment $paymentId (attempt $attempt)", e)

            if (attempt < 3 && now() < deadline - 500) {
                val delay = calculateBackoff(attempt)
                delay(delay)
                executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
            } else {
                withContext(Dispatchers.IO) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Failed after $attempt attempts: ${e.message}")
                    }
                }
            }
        }
    }

    private suspend fun waitForRateLimitAsync(deadline: Long): Boolean {
        val checkInterval = 1L

        while (now() < deadline) {
            if (rateLimiter.tick()) {
                return true
            }
            delay(checkInterval)
        }
        return false
    }

    private suspend fun executeHttpRequestAsync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID
    ) {
        val url = "http://$paymentProviderHostPort/external/process?" +
                "serviceName=$serviceName&token=$token&accountName=$accountName&" +
                "transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val response = try {
            withTimeout(30000L) {
                client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
            }
        } catch (e: TimeoutCancellationException) {
            logger.error("[$accountName] HTTP timeout for payment $paymentId")
            throw e
        }

        val body = try {
            mapper.readValue(response.body(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] Parse error for payment $paymentId: ${response.body()}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.debug("[$accountName] Payment result for txId: $transactionId, succeeded: ${body.result}")

        withContext(Dispatchers.IO) {
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
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