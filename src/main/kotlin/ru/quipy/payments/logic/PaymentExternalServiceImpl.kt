package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
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
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
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
        .executor(Executors.newFixedThreadPool(128))
        .connectTimeout(Duration.ofMillis(150))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val retryCount = 3
    private val maxRetryDelayMs = 1000L
    private val baseRetryDelayMs = 200L

    private val backupRequestCopies = 3
    private val backupRequestDelayMs = 100L
    private val requestTimeoutMs = 1500L

    private val circuitBreakerConfig = CircuitBreakerConfig.custom()
        .failureRateThreshold(10F)
        .slowCallRateThreshold(10F)
        .waitDurationInOpenState(Duration.ofSeconds(10))
        .slowCallDurationThreshold(Duration.ofSeconds(1))
        .permittedNumberOfCallsInHalfOpenState(50)
        .build()

    private val circuitBreaker = CircuitBreaker.of("paymentService", circuitBreakerConfig)


    private suspend fun waitForRateSlotUntil(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (now() >= deadline) {
                return false
            }
            delay(5)
        }
        return true
    }

    override suspend fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val idempotencyKey = transactionId.toString()

        val submittedAt = now()
        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = true,
                    transactionId = transactionId,
                    submittedAt,
                    Duration.ofMillis(submittedAt - paymentStartedAt)
                )
            }
        }

        if (!waitForRateSlotUntil(deadline)) {
            logger.error("[$accountName] Rate limit wait exceeded deadline for txId: $transactionId, payment: $paymentId")
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Rate limit wait exceeded deadline.")
                }
            }
            return
        }

        if (!ongoingWindow.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)) {
            logger.error("[$accountName] Ongoing window timeout for txId: $transactionId, payment: $paymentId")
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Ongoing window timeout.")
                }
            }
            return
        }

        if (!circuitBreaker.tryAcquirePermission()) {
            logger.error("[$accountName] Circuit breaker is open for txId: $transactionId")
            ongoingWindow.release()
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Circuit breaker opened")
                }
            }
            return
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val request = HttpRequest.newBuilder()
            .uri(
                URI(
                    "http://$paymentProviderHostPort/external/process" +
                            "?serviceName=$serviceName" +
                            "&token=$token" +
                            "&accountName=$accountName" +
                            "&transactionId=$transactionId" +
                            "&paymentId=$paymentId" +
                            "&amount=$amount"
                )
            )
            .header("x-idempotency-key", idempotencyKey)
            .timeout(Duration.ofMillis(requestTimeoutMs))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        var attemptNumber = 0
        var finished = false

        try {
            while (!finished && attemptNumber < retryCount && now() < deadline) {
                attemptNumber++

                try {
                    val timeLeft = deadline - now()
                    if (timeLeft <= 0) {
                        break
                    }

                    val startedAt = System.currentTimeMillis()
                    val response = try {
                        withTimeout(timeLeft.coerceAtMost(requestTimeoutMs)) {
                            client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                        }
                    } catch (e: Exception) {
                        null
                    }
//                    val response = raceForFirstResponse(
//                        request = request,
//                        copies = backupRequestCopies,
//                        delayBetweenCopiesMs = backupRequestDelayMs,
//                        timeoutBudgetMs = timeLeft
//                    )
                    val latencyMs = System.currentTimeMillis() - startedAt

                    if (response == null) {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt: $attemptNumber")
                        circuitBreaker.onError(latencyMs, TimeUnit.MILLISECONDS, Exception("Request timeout"))

                        if (attemptNumber >= retryCount || now() >= deadline) {
                            dbScope.launch {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                }
                            }
                            finished = true
                        }
                        continue
                    }

                    val body = try {
                        mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}"
                        )
                        ExternalSysResponse(
                            transactionId = transactionId.toString(),
                            paymentId = paymentId.toString(),
                            result = false,
                            message = e.message
                        )
                    }

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                    )

                    if (body.result) {
                        circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)

                        dbScope.launch {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                        }
                        finished = true
                    } else if (body.message == "Temporary error" && attemptNumber < retryCount && now() < deadline) {
                        circuitBreaker.onError(latencyMs, TimeUnit.MILLISECONDS, Exception(body.message))
                        delay(calculateRetryDelay(attemptNumber))
                    } else {
                        circuitBreaker.onError(latencyMs, TimeUnit.MILLISECONDS, Exception(body.message))

                        dbScope.launch {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = body.message)
                            }
                        }
                        finished = true
                    }
                } catch (e: Exception) {
                    circuitBreaker.onError(0, TimeUnit.MILLISECONDS, e)

                    when (e.cause) {
                        is SocketTimeoutException -> logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                            e
                        )
                        else -> logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                            e
                        )
                    }

                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                    finished = true
                }
            }

            if (!finished) {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                    }
                }
            }
        } finally {
            ongoingWindow.release()
        }
    }

    private suspend fun raceForFirstResponse(
        request: HttpRequest,
        copies: Int,
        delayBetweenCopiesMs: Long,
        timeoutBudgetMs: Long
    ): HttpResponse<String>? = coroutineScope {
        val firstCompletedResponse = CompletableDeferred<HttpResponse<String>?>()

        val workers = (0 until copies).map { copyIndex ->
            async(Dispatchers.IO) {
                delay(copyIndex * delayBetweenCopiesMs)

                if (firstCompletedResponse.isCompleted) {
                    return@async
                }

                try {
                    val response = withTimeout(timeoutBudgetMs) {
                        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                    }

                    firstCompletedResponse.complete(response)
                } catch (_: Exception) {
                    if (copyIndex == copies - 1 && !firstCompletedResponse.isCompleted) {
                        firstCompletedResponse.complete(null)
                    }
                }
            }
        }

        val result = withTimeoutOrNull(timeoutBudgetMs) {
            firstCompletedResponse.await()
        }

        workers.forEach { it.cancel() }
        result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun calculateRetryDelay(attempt: Int): Long {
        return minOf(
            (baseRetryDelayMs * 2.0.pow((attempt - 1).toDouble())).toLong(),
            maxRetryDelayMs
        )
    }
}

public fun now() = System.currentTimeMillis()