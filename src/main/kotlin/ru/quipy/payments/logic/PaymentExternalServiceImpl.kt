package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
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
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val processingTime = properties.averageProcessingTime

    private val client = OkHttpClient.Builder()
        .connectTimeout(2500, TimeUnit.MILLISECONDS)
        .readTimeout(1800, TimeUnit.MILLISECONDS)
        .writeTimeout(1600, TimeUnit.MILLISECONDS)
        .callTimeout(3500, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val semaphore = Semaphore(parallelRequests)
    private val meterRegistry: MeterRegistry = Metrics.globalRegistry

    private val semaphoreRequestsCounter: Counter = Counter.builder("payment.semaphore.requests.total")
        .description("Total number of requests to the semaphore")
        .tag("account", accountName)
        .tag("service", serviceName)
        .register(meterRegistry)
    private val semaphoreAcquiredCounter: Counter = Counter.builder("payment.semaphore.acquired.total")
        .description("Total number of requests that acquired the semaphore")
        .tag("account", accountName)
        .tag("service", serviceName)
        .register(meterRegistry)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        semaphoreRequestsCounter.increment()

        try {
            val acquired = semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)
            if (!acquired) {
                recordFinalFailure(paymentId, paymentStartedAt, "Semaphore timeout")
                return
            }

            semaphoreAcquiredCounter.increment()

            val toBlock = deadline - now()
            if (!rateLimiter.tickBlocking(Duration.ofMillis(toBlock))) {
                recordFinalFailure(paymentId, paymentStartedAt, "Rate limit exceeded")
                return
            }

            var currentRetry = 0
            val maxRetries = 3
            val transactionId = UUID.randomUUID()

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            while (currentRetry < maxRetries) {
                currentRetry++

                if (now() + processingTime.toMillis() > deadline) {
                    logger.error("[$accountName] Too late for payment $paymentId")
                    recordProcessingFailure(paymentId, transactionId, "Deadline exceeded")
                    break
                }

                val remainingTime = deadline - now()
                if (remainingTime < 200) {
                    recordProcessingFailure(paymentId, transactionId, "Insufficient time: ${remainingTime}ms")
                    break
                }

                val callTimeout = minOf(remainingTime - 100, 2500)
                registerTimeoutMetric(callTimeout)

                val start = now()

                val request = Request.Builder()
                    .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    .post(emptyBody)
                    .build()

                var body: ExternalSysResponse? = null
                try {
                    client.newCall(request).execute().use { response ->
                        body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] Parse error for payment $paymentId")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                        }

                        registerRequestTime(Duration.ofMillis(now() - start))

                        if (response.code == 200 && (body?.result ?: false)) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body?.message)
                            }
                            return
                        }

                        val shouldRetry = response.code == 429 || response.code in 500..599
                        registerRetryMetric(currentRetry, if (response.code == 429) "HTTP_429" else "HTTP_5XX",
                            body?.message ?: "HTTP ${response.code}")

                        if (!shouldRetry || currentRetry == maxRetries) {
                            recordProcessingFailure(paymentId, transactionId, "HTTP ${response.code}: ${body?.message}")
                            return
                        }
                    }
                } catch (e: Exception) {
                    registerRequestTime(Duration.ofMillis(now() - start))

                    val shouldRetry = e is SocketTimeoutException && currentRetry < maxRetries
                    registerRetryMetric(currentRetry, "TIMEOUT", e.message ?: "Socket timeout")

                    if (!shouldRetry) {
                        recordProcessingFailure(paymentId, transactionId,
                            if (e is SocketTimeoutException) "Timeout" else e.message ?: "Error")
                        return
                    }

                    logger.info("[$accountName] Retry attempt $currentRetry for payment $paymentId (cause=TIMEOUT)")
                }

                if (currentRetry < maxRetries) {
                    val backoff = when (currentRetry) {
                        1 -> 100L
                        2 -> 200L
                        else -> 400L
                    }
                    if (deadline - now() > backoff + 200) {
                        Thread.sleep(backoff)
                    }
                }
            }

            if (currentRetry >= maxRetries) {
                recordProcessingFailure(paymentId, transactionId, "Max retries exceeded")
            }

        } finally {
            semaphore.release()
        }
    }

    private fun recordProcessingFailure(paymentId: UUID, transactionId: UUID, reason: String) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun recordFinalFailure(paymentId: UUID, paymentStartedAt: Long, reason: String) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun registerRequestTime(timeTaken: Duration) {
        meterRegistry.counter(
            "payment.requests.time",
            Tags.of(
                "account", accountName,
                "service", serviceName,
                "time", timeTaken.toMillis().toString(),
            )
        ).increment()
    }

    private fun registerRetryMetric(retryNumber: Int, cause: String, message: String) {
        meterRegistry.counter(
            "payment.requests.retry.message",
            Tags.of(
                "account", accountName,
                "service", serviceName,
                "retry", retryNumber.toString(),
                "cause", cause,
                "message", message.take(100)
            )
        ).increment()
    }

    private fun registerTimeoutMetric(timeoutMs: Long) {
        meterRegistry.gauge(
            "payment.timeout.current_ms",
            Tags.of("account", accountName, "service", serviceName),
            timeoutMs.toDouble()
        )
    }

    override fun approximateWaitingTime(queueLength: Long): Long {
        return queueLength / rateLimitPerSec * 1000 + processingTime.toMillis()
    }

    override fun getRateLimit(): Long {
        return rateLimitPerSec.toLong()
    }

    override fun getProcessingTime(): Duration {
        return processingTime
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()