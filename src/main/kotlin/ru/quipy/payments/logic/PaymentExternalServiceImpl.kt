package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(5))
        .build()

    private val semaphore = Semaphore(parallelRequests)

    private val retryExecutor = Executors.newFixedThreadPool(4, NamedThreadFactory("payment-retry"))

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.info("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        executePaymentSimple(paymentId, amount, transactionId, deadline)
    }

    private fun executePaymentSimple(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ) {
        if (now() > deadline) {
            logFailure(paymentId, transactionId, "Deadline exceeded")
            return
        }

        val timeToWait = deadline - now()
        if (!semaphore.tryAcquire(timeToWait, TimeUnit.MILLISECONDS)) {
            logFailure(paymentId, transactionId, "Semaphore timeout")
            return
        }

        try {
            executeHttpRequest(paymentId, amount, transactionId)
        } catch (e: Exception) {
            logFailure(paymentId, transactionId, "HTTP error: ${e.message}")
            trySimpleRetry(paymentId, amount, transactionId, deadline, e)
        } finally {
            semaphore.release()
        }
    }

    private fun trySimpleRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        originalError: Exception
    ) {
        val timeLeft = deadline - now()
        if (timeLeft > 1000) {
            retryExecutor.submit {
                Thread.sleep(200)
                executePaymentSimple(paymentId, amount, transactionId, deadline)
            }
        }
    }

    private fun logFailure(paymentId: UUID, transactionId: UUID, reason: String) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun executeHttpRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID
    ) {
        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(20))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val response = client.send(request, HttpResponse.BodyHandlers.ofString())

        val body = try {
            mapper.readValue(response.body(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] Parse error for payment $paymentId: ${response.body()}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.info("[$accountName] Payment result for txId: $transactionId, succeeded: ${body.result}")

        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()