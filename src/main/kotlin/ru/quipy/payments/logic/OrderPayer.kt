package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.apigateway.TooManyRequestsError
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import ru.quipy.common.utils.LeakingBucketRateLimiter

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService
    private val paymentExecutor = ThreadPoolExecutor(
        32,
        32,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(10_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = 2000,
        window = Duration.ofMillis(500),
        bucketSize = 25000
    )

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {

        val toBlock = deadline - System.currentTimeMillis()

        if (!rateLimiter.tick()) {
            throw TooManyRequestsError(10_000)
        }

        if (toBlock <= 0) {
            throw TooManyRequestsError(10_000)
        }

        val createdAt = System.currentTimeMillis()
        executorScope.launch {
            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, amount)
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}