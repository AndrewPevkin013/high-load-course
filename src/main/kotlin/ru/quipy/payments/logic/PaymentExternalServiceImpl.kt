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
        .executor(Executors.newFixedThreadPool(2000))
        .connectTimeout(Duration.ofMillis(500))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val maxRetryCount = 3
    private val maxDelay = 1000L
    private val startDelay = 200L

    private fun calculateBackOff(attempt: Int): Long {
        if (attempt <= 0) {
            return startDelay
        }

        var factor = 1
        repeat(attempt - 1) {
            factor *= 2
        }

        val delay = startDelay * factor
        return minOf(delay, maxDelay)
    }

    private fun timeOutOrGetAccessByRateLimiter(deadline: Long): Boolean {
        val minSleepMillis = (1000L / rateLimitPerSec.coerceAtLeast(1))

        while (true) {
            val nowMillis = now()
            if (nowMillis >= deadline) {
                return false
            }

            if (rateLimiter.tick()) {
                return true
            }

            val remaining = deadline - nowMillis
            val sleepMillis = minOf(minSleepMillis, remaining)

            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis)
            }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val acquired = semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.error("[$accountName] Semaphore timeout for transactionId: $transactionId, paymentId: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout")
            }
            return
        }

        if (!timeOutOrGetAccessByRateLimiter(deadline)) {
            logger.error("[$accountName] rate limit wait overwhelmed deadline with transactionId: $transactionId, paymentId: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "rate limit wait overwhelmed deadline")
            }
            semaphore.release()
            return
        }

        try {
            var amountOfRetries = 0
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


                } finally {
                    semaphore.release()
                }
            }.exceptionally { throwable ->
                semaphore.release()

                when (throwable) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", throwable)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", throwable)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = throwable.message)
                        }
                    }
                }
                null
            }

        } catch (e: Exception) {
            semaphore.release()
            logger.error("[$accountName] Unexpected error for txId: $transactionId, payment: $paymentId", e)

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Unexpected error: ${e.message}")
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()