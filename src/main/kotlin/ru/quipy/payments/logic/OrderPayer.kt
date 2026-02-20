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
import kotlinx.coroutines.*
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
        64,
        64,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(20_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher() + SupervisorJob())

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = 4000,
        window = Duration.ofMillis(1000),
        bucketSize = 4000
    )

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {

        val now = System.currentTimeMillis()
        val toBlock = deadline - now

        if (toBlock <= 0) {
            logger.warn("Deadline already exceeded for payment $paymentId")
            throw TooManyRequestsError(1000)
        }
        val rateLimitAcquired = try {
            withTimeout(toBlock) {
                while (!rateLimiter.tick()) {
                    delay(1)
                }
                true
            }
        } catch (e: TimeoutCancellationException) {
            false
        }
        if (!rateLimitAcquired) {
            logger.warn("Rate limit timeout for payment $paymentId")
            throw TooManyRequestsError(250)
        }

        val createdAt = System.currentTimeMillis()
        executorScope.launch {
            try {
                val createdEvent = paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.debug("Payment ${createdEvent.paymentId} for order $orderId created.")
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } catch (e: Exception) {
                logger.error("Failed to process payment $paymentId", e)
            }
        }

        return createdAt
    }
}