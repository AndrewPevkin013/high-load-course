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
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.pow

// Advice: always treat time as a Duration
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
        .executor(Executors.newFixedThreadPool(100))
        .connectTimeout(Duration.ofMillis(500))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)

    private suspend fun waitTimeout(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (now() >= deadline) {
                return false
            }
            delay(2)
        }
        return true
    }

    private val retryCount = 3
    private val maxDelay = 1000L
    private val baseDelay = 200L

    // Один основной запрос + один запасной через 100 мс
    private val backupRequestsTotal = 2
    private val backupRequestDelayMs = 100L

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        // Один и тот же ключ используется и для retry, и для hedge-дублей
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

        val request = HttpRequest.newBuilder().uri(
            URI("http://$paymentProviderHostPort/external/process" + "?serviceName=$serviceName" + "&token=$token" + "&accountName=$accountName" + "&transactionId=$transactionId" + "&paymentId=$paymentId" + "&amount=$amount"))
            .header("x-idempotency-key", idempotencyKey)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        var attempt = 0
        var isProcessed = false

        semaphore.withPermit {
            while (!isProcessed && attempt < retryCount && now() < deadline) {
                attempt++

                try {
                    val remainingTime = deadline - now()
                    if (remainingTime <= 0) break

                    val response = sendWithBackupRequests(
                        request = request,
                        timeoutMs = remainingTime
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
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

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
                        is SocketTimeoutException -> logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        else -> logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
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
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun calculateDelay(attempt: Int): Long {
        return minOf((baseDelay * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelay)
    }

    private suspend fun sendWithBackupRequests(
        request: HttpRequest,
        timeoutMs: Long
    ): HttpResponse<String>? = coroutineScope {
        val firstFinishedResponse = CompletableDeferred<HttpResponse<String>?>()
        val completedWorkers = AtomicInteger(0)
        val lastFailure = AtomicReference<Throwable?>(null)

        val workers = List(backupRequestsTotal) { index ->
            async {
                if (index > 0) {
                    delay(index * backupRequestDelayMs)
                }

                if (firstFinishedResponse.isCompleted) {
                    return@async
                }

                try {
                    val response = withTimeoutOrNull(timeoutMs) {
                        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                    }

                    if (response != null && !firstFinishedResponse.isCompleted) {
                        firstFinishedResponse.complete(response)
                    }
                } catch (e: Exception) {
                    lastFailure.set(e)
                } finally {
                    val finished = completedWorkers.incrementAndGet()
                    if (finished == backupRequestsTotal && !firstFinishedResponse.isCompleted) {
                        val error = lastFailure.get()
                        if (error != null) {
                            firstFinishedResponse.completeExceptionally(error)
                        } else {
                            firstFinishedResponse.complete(null)
                        }
                    }
                }
            }
        }

        try {
            withTimeoutOrNull(timeoutMs) {
                firstFinishedResponse.await()
            }
        } finally {
            workers.forEach { it.cancel() }
        }
    }
}

public fun now() = System.currentTimeMillis()