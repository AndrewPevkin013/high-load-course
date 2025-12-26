package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
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
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    promRegistry: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val price = properties.price
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)

    private val httpExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(10_000),
        NamedThreadFactory("http-executor"),
        CallerBlockingRejectedExecutionHandler()
    )
    private val httpExecutorScope = CoroutineScope(httpExecutor.asCoroutineDispatcher());

    private val client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()


    private val scheduler = Executors.newScheduledThreadPool(100)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId, amount: $amount")
        val start = now()
        val maxRetries = 1
        val avgProcMs = requestAverageProcessingTime.toMillis()

        suspend fun attemptCall(attempt: Int) {
            val isBeneficial = amount > price * (attempt + 1)
            semaphore.withPermit {
                rateLimiter.tickBlocking()

                val request = HttpRequest.newBuilder()
                    .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                    .timeout(Duration.ofMillis(20000))   // read timeout
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build()
                try {
                    val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                    val code = response.statusCode()
                    val body = try {
                        mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.statusCode()} reason=${response.body()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    val success = (code in 200..299 && body.result)

                    if (success) {
                        logger.warn("[$accountName] [OK] txId=$transactionId payment=$paymentId")
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                        return
                    }

                    val timeLeft = deadline - now()
                    val retriableCode = code == 200 || code == 500 || code == 502 || code == 503 || code == 504

                    if (retriableCode && attempt < maxRetries && timeLeft > avgProcMs && isBeneficial) {
                        val retryAfterMs = (500L * (1L shl attempt))
                        logger.warn("[$accountName] [RETRY] txId=$transactionId payment=$paymentId will retry after $retryAfterMs ms.")
                        scheduler.schedule(
                            { httpExecutorScope.async { attemptCall(attempt + 1) } },
                            retryAfterMs,
                            TimeUnit.MILLISECONDS
                        )
                        return
                    }

                    logger.warn("[$accountName] [FAIL] txId=$transactionId payment=$paymentId code=$code msg=${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = body.message ?: "Failed")
                    }
                } catch (ex: Exception) {
                    val isTimeout = ex is SocketTimeoutException
                    val timeLeft = deadline - now()

                    if (isTimeout && attempt < maxRetries && timeLeft > avgProcMs + 500 && isBeneficial) {
                        val retryAfterMs = (500L * (1L shl attempt))
                        scheduler.schedule(
                            { httpExecutorScope.async { attemptCall(attempt + 1) } },
                            retryAfterMs,
                            TimeUnit.MILLISECONDS
                        )
                        return
                    }

                    logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId", ex)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = ex.message)
                    }
                }
            }
        }

        httpExecutorScope.async { attemptCall(0) }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun getProperties(): PaymentAccountProperties = properties

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()