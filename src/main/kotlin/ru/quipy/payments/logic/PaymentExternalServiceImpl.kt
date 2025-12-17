package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.net.http.HttpClient
import java.net.http.HttpResponse
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpRequest
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.math.pow

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val semaphore = Semaphore(parallelRequests)

    private val client = HttpClient
        .newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .executor(Executors.newFixedThreadPool(200))
        .connectTimeout(Duration.ofMillis(500))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val maxRetryCount = 3
    private val maxDelay = 1000L
    private val startDelay = 200L

    private val pendingQueue = java.util.concurrent.ConcurrentLinkedQueue<() -> Unit>()

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf((startDelay * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelay)
    }

    private fun waitForRateLimitOrTimeout(deadline: Long): Boolean {
        val minSleepMillis = (1000L / rateLimitPerSec.coerceAtLeast(1))
        var currentTime = now()

        while (currentTime < deadline) {
            if (rateLimiter.tick()) {
                return true
            }

            val remaining = deadline - currentTime
            val sleepMillis = minOf(minSleepMillis, remaining)

            if (sleepMillis > 0) {
                try {
                    Thread.sleep(sleepMillis)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return false
                }
            }

            currentTime = now()
        }
        return false
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (!rateLimiter.tick()) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceeded")
            }
            return
        }

        if (!semaphore.tryAcquire()) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "No available slots")
            }
            return
        }

        val request = HttpRequest.newBuilder().uri(
            URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(30))
            .build()

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
            try {
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                if (!body.result && now() < deadline - 100) {
                    logger.warn("[$accountName] Payment failed, scheduling retry for $paymentId")
                }

            } catch (e: Exception) {
                semaphore.release()
                logger.error("[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e)
            }
        }.exceptionally { throwable ->
            semaphore.release()

            when (throwable) {
                is SocketTimeoutException -> {
                    logger.error(
                        "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                        throwable
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        throwable
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = throwable.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()