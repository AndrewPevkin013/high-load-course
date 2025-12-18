package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.apigateway.TooManyRequestsError
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
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
        64, // пропускная способность одного потока 1/averageProccesingTime = 1/0,5 = 2 , rps = 100 , 100/2 = 50
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(10_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )
    private val rateLimitPerSec = 1100L // это рейт лимитер для внешней системы - в конфиге у нее 120 рпс - это кол-во запросов,которая ОНА в состоянии принять
                                            // очевидно,что даже с учетом того,что наш рпс 100 лучше не просаживать 20 запросов в пустую
    private val processingTimeSec = 1L
    private val executorScope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = 1100,
        window = Duration.ofMillis(1000),
        bucketSize = 20000
    )

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {

        val toBlock = deadline - System.currentTimeMillis()

        if (toBlock <= 0) {
            throw TooManyRequestsError(10_000)
        }

        if (!rateLimiter.tick()) {
            throw TooManyRequestsError(10_000)
        }

        val createdAt = System.currentTimeMillis()
        executorScope.async {
            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, amount)
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}