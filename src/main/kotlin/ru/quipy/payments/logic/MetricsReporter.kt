package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

enum class RetryCause {
    HTTP_429,
    HTTP_4XX,
    HTTP_5XX,
    TIMEOUT,
    NETWORK_ERROR,
    PARSE_ERROR,
    RATE_LIMITER,
    PARALLEL_LIMIT,
    DEADLINE,
    UNKNOWN
}

@Component
class MetricsReporter(private val meterRegistry: MeterRegistry) {

    private val totalRetryCounter = meterRegistry.counter("payment.requests.retries")

    fun incrementRetry(account: String, cause: RetryCause) {
        meterRegistry.counter(
            "payment.requests.retries",
            Tags.of("account", account, "cause", cause.name)
        ).increment()
        totalRetryCounter.increment()
    }

    fun recordPaymentStarted(account: String) {
        meterRegistry.counter("payment.started", Tags.of("account", account)).increment()
    }

    fun recordRateLimiterResult(account: String, allowed: Boolean) {
        meterRegistry.counter(
            "payment.rate_limiter",
            Tags.of("account", account, "result", if (allowed) "allowed" else "blocked")
        ).increment()
    }

    fun recordParallelLimitBlocked(account: String) {
        meterRegistry.counter("payment.parallel_limit_blocked", Tags.of("account", account)).increment()
    }

    fun recordHttpResponse(account: String, statusCode: Int) {
        val tag = when (statusCode) {
            in 200..299 -> "2xx"
            in 400..499 -> if (statusCode == 429) "429" else "4xx"
            in 500..599 -> "5xx"
            else -> "other"
        }
        meterRegistry.counter("payment.http_responses", Tags.of("account", account, "status", tag)).increment()
    }

    private val httpRequestTimer = Timer.builder("payment.http_request_duration")
        .description("Duration of external HTTP payment requests")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(meterRegistry)

    fun recordHttpRequestDuration(account: String, durationMs: Long) {
        httpRequestTimer.record(durationMs, TimeUnit.MILLISECONDS)
    }

    fun recordParseError(account: String) {
        meterRegistry.counter("payment.parse_errors", Tags.of("account", account)).increment()
    }

    fun recordDeadlineExceeded(account: String) {
        meterRegistry.counter("payment.deadline_exceeded", Tags.of("account", account)).increment()
    }

    private val esUpdateTimer = Timer.builder("payment.es_update_duration")
        .description("Duration of EventSourcingService update calls")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(meterRegistry)

    fun recordEsUpdateDuration(durationMs: Long) {
        esUpdateTimer.record(durationMs, TimeUnit.MILLISECONDS)
    }

    private val timeoutGaugesByAccount = ConcurrentHashMap<String, AtomicLong>()

    fun updateCurrentTimeout(account: String, timeoutMs: Long) {
        val holder = timeoutGaugesByAccount.computeIfAbsent(account) { acc ->
            val atomic = AtomicLong(timeoutMs)
            Gauge.builder("payment.timeout.current_ms", atomic) { it.get().toDouble() }
                .description("Current read/overall timeout used for external payment requests")
                .tag("account", acc)
                .register(meterRegistry)
            atomic
        }
        holder.set(timeoutMs)
    }

    fun registerExecutorMetrics(executor: ThreadPoolExecutor) {
        Gauge.builder("payment.executor.queue.size", executor.queue) { it.size.toDouble() }
            .description("Current queue size of payment executor")
            .register(meterRegistry)

        Gauge.builder("payment.executor.active.threads", executor) { it.activeCount.toDouble() }
            .description("Active threads in payment executor")
            .register(meterRegistry)

        Gauge.builder("payment.executor.pool.size", executor) { it.poolSize.toDouble() }
            .description("Current pool size of payment executor")
            .register(meterRegistry)

        Gauge.builder("payment.executor.max.pool.size", executor) { it.maximumPoolSize.toDouble() }
            .description("Maximum pool size of payment executor")
            .register(meterRegistry)
    }
}