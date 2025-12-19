package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import java.net.SocketTimeoutException
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
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val semaphore = Semaphore(parallelRequests)

    private val scheduler = Executors.newScheduledThreadPool(4, NamedThreadFactory("payment-retry-scheduler"))

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.info("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        CompletableFuture.runAsync {
            executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, 1)
        }
    }

    private fun executePaymentWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        if (now() > deadline) {
            logger.error("[$accountName] Deadline exceeded for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
            }
            return
        }

        try {
            if (!waitForRateLimit(deadline)) {
                logger.error("[$accountName] Rate limit timeout for payment $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Rate limit timeout")
                }
                return
            }

            if (!semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)) {
                logger.error("[$accountName] Semaphore timeout for payment $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout")
                }
                return
            }

            try {
                executeHttpRequestSync(paymentId, amount, transactionId)
            } finally {
                semaphore.release()
            }

        } catch (e: Exception) {
            logger.error("[$accountName] Error processing payment $paymentId (attempt $attempt)", e)

            if (attempt < 3 && now() < deadline - 300) {
                val delay = calculateBackoff(attempt)
                scheduler.schedule({
                    executePaymentWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                }, delay, TimeUnit.MILLISECONDS)
            } else {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Failed after $attempt attempts: ${e.message}")
                }
            }
        }
    }

    private fun waitForRateLimit(deadline: Long): Boolean {
        val minSleep = 1000L / rateLimitPerSec.coerceAtLeast(1)
        var currentTime = now()

        while (currentTime < deadline) {
            if (rateLimiter.tick()) {
                return true
            }

            val remaining = deadline - currentTime
            val sleepTime = minOf(minSleep, remaining)

            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return false
                }
            }

            currentTime = now()
        }
        return false
    }

    private fun executeHttpRequestSync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID
    ) {
        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val response = client.send(request, HttpResponse.BodyHandlers.ofString())

        val body = try {
            mapper.readValue(response.body(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] Parse error for payment $paymentId: ${response.body()}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.info("[$accountName] Payment result for txId: $transactionId, succeeded: ${body.result}")

        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
    }

    private fun calculateBackoff(attempt: Int): Long {
        return when (attempt) {
            1 -> 200L
            2 -> 400L
            else -> 800L
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()