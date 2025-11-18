package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests

    // ограничение параллельности – как у другой команды через OngoingWindow,
    // только у нас это обычный Semaphore
    private val semaphore = Semaphore(parallelRequests)

    // простой sync-клиент, как и был
    private val client = OkHttpClient.Builder()
        .connectTimeout(2500, TimeUnit.MILLISECONDS)
        .readTimeout(1800, TimeUnit.MILLISECONDS)
        .writeTimeout(1600, TimeUnit.MILLISECONDS)
        .callTimeout(3500, TimeUnit.MILLISECONDS)
        .build()

    // тот же sliding-window, но используем tick() + ожидание до дедлайна,
    // как waitRateLimitOrTimeout у другой команды
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private fun waitRateLimitOrTimeout(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (now() >= deadline) {
                return false
            }
            Thread.sleep(5)
        }
        return true
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val maxAttempts = 3

        // как у другой команды – один transactionId и один logSubmission на весь платёж
        val transactionId = UUID.randomUUID()
        val createdAt = now()

        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true,
                transactionId = transactionId,
                startedAt = createdAt,
                spentInQueueDuration = Duration.ofMillis(createdAt - paymentStartedAt)
            )
        }

        // сначала проверяем, успеем ли вообще что-то делать
        var remainingTime = deadline - now()
        if (remainingTime < 200) {
            recordProcessingFailure(paymentId, transactionId, "Insufficient time: ${remainingTime}ms")
            return
        }

        // ограничение по параллельности – захватываем слот один раз на весь платёж
        val acquired = semaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)
        if (!acquired) {
            recordProcessingFailure(paymentId, transactionId, "Semaphore timeout")
            return
        }

        try {
            // ждём rate-limit, как waitRateLimitOrTimeout у другой команды
            if (!waitRateLimitOrTimeout(deadline)) {
                recordProcessingFailure(paymentId, transactionId, "Rate limit wait exceeded deadline")
                return
            }

            var attempt = 0

            while (attempt < maxAttempts) {
                attempt++

                remainingTime = deadline - now()
                if (remainingTime < 200) {
                    recordProcessingFailure(paymentId, transactionId, "Insufficient time: ${remainingTime}ms")
                    return
                }

                val callTimeout = minOf(remainingTime - 100, 2500)
                metricsReporter.updateCurrentTimeout(accountName, callTimeout.toLong())

                val request = Request.Builder().run {
                    url(
                        "http://$paymentProviderHostPort/external/process" +
                                "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                                "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                    )
                    post(emptyBody)
                }.build()

                try {
                    client.newCall(request).execute().use { response ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] Parse error for payment $paymentId")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                        }

                        if (response.code == 200 && body.result) {
                            // как у них – фиксируем итог обработки в любом случае
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                            return
                        }

                        val shouldRetry = response.code == 429 || response.code in 500..599
                        val cause = if (response.code == 429) RetryCause.HTTP_429 else RetryCause.HTTP_5XX
                        metricsReporter.incrementRetry(accountName, cause)

                        if (!shouldRetry || attempt == maxAttempts) {
                            recordProcessingFailure(
                                paymentId,
                                transactionId,
                                "HTTP ${response.code}: ${body.message}"
                            )
                            return
                        }

                        // простой backoff, как и был
                        val backoff = when (attempt) {
                            1 -> 100L
                            2 -> 200L
                            else -> 400L
                        }

                        remainingTime = deadline - now()
                        if (remainingTime > backoff + 200) {
                            Thread.sleep(backoff)
                        }
                    }
                } catch (e: Exception) {
                    val shouldRetry = e is SocketTimeoutException && attempt < maxAttempts
                    if (!shouldRetry) {
                        recordProcessingFailure(
                            paymentId,
                            transactionId,
                            if (e is SocketTimeoutException) "Timeout" else e.message ?: "Error"
                        )
                        return
                    }

                    metricsReporter.incrementRetry(accountName, RetryCause.TIMEOUT)
                    logger.info(
                        "[$accountName] Retry attempt ${attempt + 1} for payment $paymentId (cause=TIMEOUT)"
                    )
                    // как в твоём коде – ретрай таймаута без доп. паузы (или можно добавить backoff, если нужно)
                }
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

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
