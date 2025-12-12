package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okio.use
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {


    private val rps = properties.rateLimitPerSec;
    private  val expectedProccesingTime = 10_000L;
    private val ioSlots: Int = ((rps * expectedProccesingTime) / 1000.0 * 1.2).toInt()

    private  val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
    private   val emptyBody = RequestBody.create(null, ByteArray(0))
    private  val mapper = ObjectMapper().registerKotlinModule()

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val dispatcher = okhttp3.Dispatcher().apply {
        maxRequests = ioSlots
        maxRequestsPerHost = ioSlots
    }

    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .build()

//    private val semaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val maxAttempts = 10
        val transactionId = UUID.randomUUID()

        fun attempt(at: Int) {
            val remainingTime = deadline - now()
            if (remainingTime < 2000) {
                recordFinalFailure(paymentId, paymentStartedAt, "Insufficient time: ${remainingTime}ms")
                return
            }

            val toBlock = deadline - System.currentTimeMillis()
            if (!rateLimiter.tick()) {
                recordFinalFailure(paymentId, paymentStartedAt, "Rate limit exceeded")
                return
            }

//            if (!semaphore.tryAcquire()) {
//                recordFinalFailure(paymentId, paymentStartedAt, "No I/O slots")
//                return
//            }

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val callTimeout = (expectedProccesingTime + 2_000)
                .coerceAtMost(remainingTime - 1000)
            metricsReporter.updateCurrentTimeout(accountName, callTimeout)

            val request = Request.Builder()
                .url("http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                        "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()

            val call = client.newCall(request)
            call.timeout().timeout(callTimeout, TimeUnit.MILLISECONDS)


            val started = System.nanoTime()

            call.enqueue(object : okhttp3.Callback {
                override fun onFailure(call: okhttp3.Call, e: java.io.IOException) {
                    val shouldRetry = (e is SocketTimeoutException) && at < maxAttempts &&
                            (deadline - now()) > 3000

                    if (!shouldRetry) {
                        recordProcessingFailure(paymentId, transactionId,
                            if (e is SocketTimeoutException) "Timeout" else e.message ?: "Error")
                        return
                    }
                    metricsReporter.incrementRetry(accountName, RetryCause.TIMEOUT)
                    scheduleBackoff(at) { attempt(at + 1) }
                }

                override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                    response.use {
                        val body = runCatching {
                            mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                        }.getOrElse {
                            logger.error("[$accountName] Parse error for payment $paymentId")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                        }


                        if (it.code == 200 && body.result) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                            return
                        }

                        val shouldRetry = it.code == 429 || it.code in 500..599
                        val cause = if (it.code == 429) RetryCause.HTTP_429 else RetryCause.HTTP_5XX
                        metricsReporter.incrementRetry(accountName, cause)

                        if (!shouldRetry || at == maxAttempts) {
                            recordProcessingFailure(paymentId, transactionId, "HTTP ${it.code}: ${body.message}")
//                            semaphore.release()
                            return
                        }

                        scheduleBackoff(at) { attempt(at + 1) }
//                        semaphore.release()
                    }
                }
            })
        }

        attempt(1)
    }

    object Schedulers {
        val backoff: ScheduledExecutorService = ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors().coerceAtMost(4)
        ).apply {
            removeOnCancelPolicy = true
        }
    }


    private fun scheduleBackoff(attempt: Int, action: () -> Unit) {
        val backoff = when (attempt) { 1 -> 100L; 2 -> 200L; else -> 400L }
        Schedulers.backoff.schedule(action, backoff, TimeUnit.MILLISECONDS)
    }

    private fun msSince(ns: Long) = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ns)


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

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()