package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(private val dbScope: CoroutineScope) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        50,
        100,
        70000,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(5500),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.DiscardOldestPolicy()
    )
    val executorScope = CoroutineScope(SupervisorJob() + paymentExecutor.asCoroutineDispatcher())

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        executorScope.launch {
            if (now() >= deadline) {
                logger.warn("Payment $paymentId deadline already exceeded, skipping")
                return@launch
            }

            dbScope.launch {
                paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.trace("Payment $paymentId for order $orderId created.")
            }

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}