package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.mq.dto.TradeExecuted;
import com.CCM_EV.admin.repository.ReportingRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class TradeConsumer {
    private final ReportingRepository repo;
    private final MeterRegistry meters;

    @RabbitListener(queues = "admin.trade-executed.v1")
    @Transactional
    public void onTradeExecuted(TradeExecuted evt){
        if (!repo.tryMarkConsumed(evt.event_id())) return;
        repo.upsertTrade(
                evt.data().order_id(), evt.data().buyer_id(), evt.data().seller_id(),
                evt.data().listing_id(), evt.data().quantity(), evt.data().unit(),
                evt.data().unit_price(), evt.data().amount(), evt.data().currency(), evt.data().executed_at()
        );
        meters.counter("admin.events.consumed", "type","trade_executed").increment();
    }
}
