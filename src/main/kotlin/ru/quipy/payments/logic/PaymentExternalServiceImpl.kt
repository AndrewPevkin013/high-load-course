package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.math.pow


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient
        .newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .executor(Executors.newFixedThreadPool(100))
        .connectTimeout(Duration.ofMillis(500))
        .build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val submittedCounter = Counter.builder("payments_submitted_total").register(meterRegistry)
    private val sentQueriesSuccess = Counter.builder("payments_success").register(meterRegistry)
    private val retryCounter = Counter.builder("payments_retries_total").register(meterRegistry)

    private val requestLatency = DistributionSummary.builder("request_latency")
        .description("Request latency.")
        .publishPercentiles(0.5, 0.8, 0.90, 0.95, 0.99)
        .register(meterRegistry)


    private suspend fun waitRateLimitOrTimeout(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (now() >= deadline) return false
            delay(5)
        }
        return true
    }

    private val retryCount = 3
    private val maxDelay = 1000L
    private val baseDelay = 200L

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        submittedCounter.increment()
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        if (!waitRateLimitOrTimeout(deadline)) {
            logger.error("[$accountName] Rate limit wait exceeded deadline for txId: $transactionId, payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit wait exceeded deadline.")
            }
            return
        }

        val request = HttpRequest.newBuilder().uri(
            URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        var attempt = 0
        var processed = false

        while (!processed && attempt < retryCount && now() < deadline) {
            attempt++

            try {
                val remainingTime = deadline - now()
                if (remainingTime <= 0) break

                val start = System.currentTimeMillis()
                val response = withTimeoutOrNull(remainingTime) {
                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                }

                if (response == null) {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt: $attempt")
                    if (attempt >= retryCount || now() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                        processed = true
                    }
                    continue
                }

                val latency = (System.currentTimeMillis() - start).toDouble()
                requestLatency.record(latency)

                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                if (body.result) {
                    sentQueriesSuccess.increment()
                    paymentESService.update(paymentId) {
                        it.logProcessing(true, now(), transactionId, reason = body.message)
                    }
                    processed = true
                } else if (body.message == "Temporary error" && attempt < retryCount && now() < deadline) {
                    retryCounter.increment()
                    delay(exponentialBackoffDelay(attempt))
                } else {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = body.message)
                    }
                    processed = true
                }

            } catch (e: Exception) {
                when (e.cause) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    }
                }
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
                processed = true
            }
        }

        if (!processed) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
            }
        }

    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf((baseDelay * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelay)
    }
}

public fun now() = System.currentTimeMillis()