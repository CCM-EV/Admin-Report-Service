package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.mq.dto.UserLoggedIn;
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
public class LoginConsumer {
    private final ReportingRepository repo;
    private final MeterRegistry meters;

    @RabbitListener(queues = "admin.user-loggedin.v1")
    @Transactional
    public void onUserLoggedIn(UserLoggedIn evt){
        log.info("Received user login event: {}", evt.event_id());
        if(!repo.tryMarkConsumed(evt.event_id())) {
            log.warn("Duplicate user login event: {}", evt.event_id());
            return;
        }

        repo.insertUserActivity(
                evt.data().user_id(),
                "LOGIN",
                String.format("{\"username\":\"%s\",\"ip\":\"%s\",\"user_agent\":\"%s\"}", 
                    evt.data().username(), 
                    evt.data().ip_address() != null ? evt.data().ip_address() : "unknown",
                    evt.data().user_agent() != null ? evt.data().user_agent() : "unknown"),
                evt.data().login_at()
        );
        
        meters.counter("admin.events.consumed", "type","user_loggedin").increment();
        log.info("User login event processed: {}", evt.event_id());
    }
}
