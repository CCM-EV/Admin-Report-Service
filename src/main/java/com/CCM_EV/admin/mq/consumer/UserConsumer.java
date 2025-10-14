package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.mq.dto.UserRegistered;
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
public class UserConsumer {
    private final ReportingRepository repo;
    private final MeterRegistry meters;

    @RabbitListener(queues = "admin.user-registered.v1")
    @Transactional
    public void onUserRegistered(UserRegistered evt){
        log.info("Received user registration event: {}", evt.event_id());
        if(!repo.tryMarkConsumed(evt.event_id())) {
            log.warn("Duplicate user registration event: {}", evt.event_id());
            return;
        }
        
        repo.upsertUser(
                evt.data().user_id(),
                evt.data().username(),
                evt.data().email(),
                evt.data().role(),
                evt.data().organization_name(),
                evt.data().region(),
                evt.data().created_at()
        );

        repo.insertUserActivity(
                evt.data().user_id(),
                "REGISTER",
                String.format("{\"username\":\"%s\",\"email\":\"%s\",\"role\":\"%s\"}", 
                    evt.data().username(), evt.data().email(), evt.data().role()),
                evt.occurred_at()
        );
        
        meters.counter("admin.events.consumed", "type","user_registered").increment();
        log.info("User registration event processed: {}", evt.event_id());
    }
}
