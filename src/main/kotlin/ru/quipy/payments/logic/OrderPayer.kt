package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.apigateway.TooManyRequestsError
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import jakarta.annotation.PostConstruct

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var metricsReporter: MetricsReporter

    private val paymentExecutor = ThreadPoolExecutor(
        50,
        50,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(30_000),
        NamedThreadFactory("payment-submission-executor"),
    )

    private val executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())

    @PostConstruct
    fun init() {
        metricsReporter.registerExecutorMetrics(paymentExecutor)
    }

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val now = System.currentTimeMillis()
        if (now >= deadline) {
            throw TooManyRequestsError(10_000)
        }

        val createdAt = now
        executorScope.launch {
            val startEs = System.currentTimeMillis()
            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, amount)
            }
            metricsReporter.recordEsUpdateDuration(System.currentTimeMillis() - startEs)
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}