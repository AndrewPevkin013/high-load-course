package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.RequestBody
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
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.pow

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val dbScope: CoroutineScope
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
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
    private val semaphore = Semaphore(parallelRequests, true)

    private val retryCount = 3
    private val maxDelay = 250L
    private val baseDelay = 100L

    private val hedgeCopies = 2
    private val hedgeSpacingMs = 1000L
    private val requestTimeoutMs = 1700L

    private suspend fun waitTimeout(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (now() >= deadline) {
                return false
            }
            delay(2)
        }
        return true
    }

    private fun tryAcquireSlot(deadline: Long): Boolean {
        val waitMs = deadline - now()
        if (waitMs <= 0) {
            return false
        }
        return semaphore.tryAcquire(waitMs, TimeUnit.MILLISECONDS)
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
                    transactionId,
                    submittedAt,
                    Duration.ofMillis(submittedAt - paymentStartedAt)
                )
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        if (!waitTimeout(deadline)) {
            logger.error("[$accountName] Rate limit wait exceeded deadline for txId: $transactionId, payment: $paymentId")
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Rate limit wait exceeded deadline.")
                }
            }
            return
        }

        if (!tryAcquireSlot(deadline)) {
            logger.error("[$accountName] Parallel slot wait exceeded deadline for txId: $transactionId, payment: $paymentId")
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Parallel slot wait exceeded deadline.")
                }
            }
            return
        }

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

        var attempt = 0
        var isProcessed = false

        try {
            while (!isProcessed && attempt < retryCount && now() < deadline) {
                attempt++

                try {
                    val remainingTime = deadline - now()
                    if (remainingTime <= 0) {
                        break
                    }

                    val response = sendWithHedging(
                        request = request,
                        deadline = deadline
                    )

                    if (response == null) {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt: $attempt")
                        if (attempt >= retryCount || now() >= deadline) {
                            dbScope.launch {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                }
                            }
                            isProcessed = true
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
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message
                        )
                    }

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                    )

                    if (body.result) {
                        dbScope.launch {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                        }
                        isProcessed = true
                    } else if (body.message == "Temporary error" && attempt < retryCount && now() < deadline) {
                        delay(calculateDelay(attempt))
                    } else {
                        dbScope.launch {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = body.message)
                            }
                        }
                        isProcessed = true
                    }
                } catch (e: Exception) {
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
                    isProcessed = true
                }
            }

            if (!isProcessed) {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                    }
                }
            }
        } finally {
            semaphore.release()
        }
    }

    private suspend fun sendWithHedging(
        request: HttpRequest,
        deadline: Long
    ): HttpResponse<String>? = coroutineScope {
        val winner = CompletableDeferred<HttpResponse<String>?>()
        val finishedCount = AtomicInteger(0)

        fun completeIfAllFailed() {
            if (finishedCount.incrementAndGet() == hedgeCopies && !winner.isCompleted) {
                winner.complete(null)
            }
        }

        val jobs = List(hedgeCopies) { copyIndex ->
            async {
                if (copyIndex > 0) {
                    delay(copyIndex * hedgeSpacingMs)
                }

                if (winner.isCompleted) {
                    return@async
                }

                val timeLeft = deadline - now()
                if (timeLeft <= 0) {
                    completeIfAllFailed()
                    return@async
                }

                val requestBudget = minOf(timeLeft, requestTimeoutMs)

                try {
                    val response = withTimeoutOrNull(requestBudget) {
                        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                    }

                    if (response != null) {
                        winner.complete(response)
                    } else {
                        completeIfAllFailed()
                    }
                } catch (_: Exception) {
                    completeIfAllFailed()
                }
            }
        }

        try {
            val totalTimeLeft = deadline - now()
            if (totalTimeLeft <= 0) {
                null
            } else {
                withTimeoutOrNull(totalTimeLeft) {
                    winner.await()
                }
            }
        } finally {
            jobs.forEach { it.cancel() }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun calculateDelay(attempt: Int): Long {
        return minOf((baseDelay * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelay)
    }
}

public fun now() = System.currentTimeMillis()