package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.Executors
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.async



class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
//        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()
    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val maxRetryCount = 3
    private val maxDelay = 1000L
    private val startDelay = 200L

    private fun calculateBackOff(attempt: Int): Long {

        if (attempt <= 0) {
            return startDelay;
        }

        var factor = 1;

        repeat(attempt - 1) {
            factor *= 2;
        }

        val delay = startDelay * factor;

        if (delay > maxDelay) {
            return  maxDelay
        }

        return delay;
    }

    private fun timeOutOrGetAccessByRateLimiter(deadline: Long): Boolean {

        val minSleepMillis = (1000L / rateLimitPerSec.coerceAtLeast(1)); // один квант времени между запросами ~~ 1 / rateLimitPerSec сек

        while (true) {

            val nowMillis = now();

            if (nowMillis >= deadline) {
                return false;
            }

            if (rateLimiter.tick()) {
                return true;
            }

            val remaining = deadline - nowMillis;

            val sleepMillis = minOf(minSleepMillis, remaining);

            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
        }
    }

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        logger.warn("[$accountName] Submitting payment request for payment $paymentId");

        val transactionId = UUID.randomUUID();

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt));
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId");

        ongoingWindow.acquire();

        if (!timeOutOrGetAccessByRateLimiter(deadline)) {

            logger.error("[$accountName] rate limit wait overwhelmed deadline with transactionId: $transactionId, paymentId: $paymentId");

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "rate limit wait overwhelmed deadline");
            }

            return;
        }

        try {
            var amountOfRetries = 0;

            val toBlock = deadline - System.currentTimeMillis()
            var repeat = true
            while (toBlock >=0 && repeat) {
//                try {
                    ++amountOfRetries;

                    val request = HttpRequest.newBuilder()
                        .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                        .timeout(Duration.ofMillis(20000))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build()

                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        }catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                        }

                        ongoingWindow.release()

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")


                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message);
                        }

                        if (body.result || (amountOfRetries == maxRetryCount)) {
                            repeat = false
                        }

                        Thread.sleep(calculateBackOff(amountOfRetries));
                    }
//                }

//                catch (e: java.io.InterruptedIOException) {
//
//                    logger.warn("[$accountName] request stopped by a client timeout for transactionId=$transactionId (in attempt $amountOfRetries of $maxRetryCount)")
//
//                    if (amountOfRetries < maxRetryCount && now() < deadline) {
//                        Thread.sleep(calculateBackOff(amountOfRetries))
//                        continue
//                    }
//                    else {
//                        paymentESService.update(paymentId) {
//                            it.logProcessing(false, now(), transactionId, reason = "Client timeout after $maxRetryCount retries.")
//                        }
//                    }
//                }
            }

        }
        catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
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