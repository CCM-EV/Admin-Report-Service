package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.mq.dto.CreditIssued;
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
public class IssuanceConsumer {
    private final ReportingRepository repo;
    private final MeterRegistry meters;

    @RabbitListener(queues = "admin.issuance.events")
    @Transactional
    public void onIssuanceCreated(CreditIssued evt){
        log.info("Received credit issuance event: {}", evt.event_id());
        if(!repo.tryMarkConsumed(evt.event_id())) {
            log.warn("Duplicate issuance event: {}", evt.event_id());
            return;
        }
        
        repo.upsertIssuance(
                evt.data().issuance_id(),
                evt.data().user_id(),
                evt.data().request_id(),
                evt.data().quantity_tco2e(),
                evt.data().distance_km(),
                evt.data().energy_kwh(),
                evt.data().co2_avoided_kg(),
                evt.data().issued_at()
        );
        
        meters.counter("admin.events.consumed", "type","credit_issued").increment();
        log.info("Credit issuance event processed: {}", evt.event_id());
    }
}
