package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private lateinit var paymentExecutor: ThreadPoolExecutor
    private lateinit var executorScope: CoroutineScope
    private var averageProcessingTime: Long = 0
    private var rateLimitPerSec: Int = 0
    private var parallelRequests: Int = 0

    private lateinit var rateLimiter: RateLimiter

    @PostConstruct
    private fun initialize() {
        paymentExecutor = ThreadPoolExecutor(
            16,
            16,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(10_000),
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
        executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())
        rateLimiter =
            LeakingBucketRateLimiter(
                rate = 1100,
                window = Duration.ofMillis(1000),
                bucketSize = 20000
            )
    }

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        if (!rateLimiter.tick()) {
            throw TooManyRequestsError(10000)
        }

        executorScope.async {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}

class TooManyRequestsError(val millisToRetry: Long) : RuntimeException()