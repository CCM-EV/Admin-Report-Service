package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.mq.dto.PaymentCompleted;
import com.CCM_EV.admin.repository.ReportingRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConsumer {
    private final ReportingRepository repo;
    private final MeterRegistry meters;

    @RabbitListener(queues = "admin.payment-completed.v1")
    @Transactional
    public void onPaymentReceived(PaymentCompleted evt){
        log.info("Received payment event: {}", evt.event_id());
        if(!repo.tryMarkConsumed(evt.event_id())) {
            log.warn("Duplicate payment event: {}", evt.event_id());
            return;
        }
        
        repo.upsertPayment(
                evt.data().payment_id(),
                evt.data().order_id(),
                evt.data().amount(),
                evt.data().currency(),
                evt.data().status(),
                evt.data().payment_method(),
                evt.data().completed_at()
        );
        
        meters.counter("admin.events.consumed", "type","payment_completed").increment();
        log.info("Payment event processed: {}", evt.event_id());
    }
}
