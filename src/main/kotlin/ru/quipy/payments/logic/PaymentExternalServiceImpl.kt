package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import okio.use
import org.slf4j.LoggerFactory
import org.springframework.aot.hint.TypeReference.listOf
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
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .connectionPool(ConnectionPool(200, 5, TimeUnit.MINUTES))
        .dispatcher(dispatcher)
        .build()

    private val semaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val startTime = now()

        val initialRemainingTime = deadline - startTime
        if (initialRemainingTime < 5000) {
            recordFinalFailure(paymentId, paymentStartedAt, "No time")
            return
        }

        val timeToBlock = deadline - System.currentTimeMillis()
        val acquired = semaphore.tryAcquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            recordFinalFailure(paymentId, paymentStartedAt, "No capacity")
            return
        }

        if (!rateLimiter.tick()) {
            semaphore.release()
            recordFinalFailure(paymentId, paymentStartedAt, "Rate limit")
            return
        }

        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, startTime, Duration.ofMillis(startTime - paymentStartedAt))
        }

        val callTimeout = minOf(initialRemainingTime - 2000, 15000L)


        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process" +
                    "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                    "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(RequestBody.create(null, ByteArray(0)))
            .build()

        val clientWithTimeouts = client.newCall(request)
        clientWithTimeouts.timeout().timeout(callTimeout, TimeUnit.MILLISECONDS)

        clientWithTimeouts.enqueue(object : okhttp3.Callback {
            override fun onFailure(call: okhttp3.Call, e: java.io.IOException) {
                semaphore.release()
                val timeLeft = deadline - now()

                if (e is SocketTimeoutException && timeLeft > 5000) {
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                } else {
                    recordProcessingFailure(paymentId, transactionId, "Failed: ${e.message}")
                }
            }

            override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                semaphore.release()
                response.use {
                    val body = try {
                        mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                    }

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        })
    }

    object Schedulers {
        val backoff: ScheduledExecutorService = ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors().coerceAtMost(4)
        ).apply {
            removeOnCancelPolicy = true
        }
    }


    private fun scheduleBackoff(attempt: Int, action: () -> Unit) {
        val backoff = when (attempt) { 1 -> 50L; 2 -> 100L; else -> 200L }
        Schedulers.backoff.schedule(action, backoff, TimeUnit.MILLISECONDS)
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

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()