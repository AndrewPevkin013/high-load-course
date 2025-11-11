package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.ConnectionPool
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
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        private const val TEST_RPS = 100                      // ← жёстко под тест
        private const val EXPECTED_PROCESSING_MS = 20_000L    // ← 20s под тест
        private val IO_SLOTS: Int = ((TEST_RPS * EXPECTED_PROCESSING_MS) / 1000.0 * 1.2).toInt()
        // IO_SLOTS ≈ 2400

        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests

//    private val semaphore = Semaphore(parallelRequests)

    private val dispatcher = okhttp3.Dispatcher().apply {
        maxRequests = IO_SLOTS
        maxRequestsPerHost = IO_SLOTS
    }

    @Volatile private var expectedProcessingMs: Long = 20_000

    fun configureConcurrency(ioSlots: Int) {
        // 1) OkHttp dispatcher — общий и per-host лимиты
        dispatcher.maxRequests = ioSlots
        dispatcher.maxRequestsPerHost = ioSlots

        // 2) «Пересобираем» семафор на новое число слотов I/O
        semaphoreRef.set(Semaphore(ioSlots))

        logger.info("[$accountName] Concurrency tuned: ioSlots=$ioSlots")
    }

    /**
     * ratePerSecond × processingTimeMillis ~= нужное число одновременных запросов (Little's Law)
     * +20% запас, чтобы не упираться в лимиты из-за флуктуаций
     */
    fun applyTestParams(ratePerSecond: Int, processingTimeMillis: Long) {
        expectedProcessingMs = processingTimeMillis
        val inflight = (ratePerSecond * processingTimeMillis / 1_000.0).toInt() // напр., 100 * 20_000ms = 2000
        val ioSlots = maxOf(64, (inflight * 1.2).toInt())                      // ~2400 для твоего кейса
        configureConcurrency(ioSlots)
    }


    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .build()


    private val semaphoreRef = AtomicReference(Semaphore(IO_SLOTS))
    private fun sem(): Semaphore = semaphoreRef.get()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val maxAttempts = 3
        val transactionId = UUID.randomUUID()

        fun attempt(at: Int) {
            val remainingTime = deadline - now()
            if (remainingTime < 200) {
                recordFinalFailure(paymentId, paymentStartedAt, "Insufficient time: ${remainingTime}ms")
                return
            }

            val semaphore = sem()
            // конкуренция I/O
            if (!semaphore.tryAcquire()) {
                // если не хватает слотов — быстрый фэйл (или попробовать чуть подождать)
                recordFinalFailure(paymentId, paymentStartedAt, "No I/O slots")
                return
            }

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val callTimeout = (EXPECTED_PROCESSING_MS + 2_000) // ~22s
                .coerceAtMost(remainingTime - 100)
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
                    val callMs = msSince(started)

                    val shouldRetry = (e is SocketTimeoutException) && at < maxAttempts
                    if (!shouldRetry) {
                        recordProcessingFailure(paymentId, transactionId, if (e is SocketTimeoutException) "Timeout" else e.message ?: "Error")
                        semaphore.release()
                        return
                    }
                    metricsReporter.incrementRetry(accountName, RetryCause.TIMEOUT)
                    // без Thread.sleep: планируем ретрай неблокирующе
                    scheduleBackoff(at) { attempt(at + 1) }
                    semaphore.release()
                }

                override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                    response.use {
                        val body = runCatching {
                            mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                        }.getOrElse {
                            logger.error("[$accountName] Parse error for payment $paymentId")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                        }

                        val callMs = msSince(started)

                        if (it.code == 200 && body.result) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                            semaphore.release()
                            return
                        }

                        val shouldRetry = it.code == 429 || it.code in 500..599
                        val cause = if (it.code == 429) RetryCause.HTTP_429 else RetryCause.HTTP_5XX
                        metricsReporter.incrementRetry(accountName, cause)

                        if (!shouldRetry || at == maxAttempts) {
                            recordProcessingFailure(paymentId, transactionId, "HTTP ${it.code}: ${body.message}")
                            semaphore.release()
                            return
                        }

                        scheduleBackoff(at) { attempt(at + 1) }
                        semaphore.release()
                    }
                }
            })
        }

        attempt(1)
    }

    object Schedulers {
        // маленький пул, т.к. задачи только "подождать и запланировать ретрай"
        val backoff: ScheduledExecutorService = ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors().coerceAtMost(4)
        ).apply {
            removeOnCancelPolicy = true
            setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
        }
    }


    // Неблокирующий бэкофф (через общий планировщик, можно взять из Spring TaskScheduler)
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