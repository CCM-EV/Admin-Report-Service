package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.metrics.AdminMetricsService;
import com.CCM_EV.admin.mq.dto.common.PaymentEventDTO;
import com.CCM_EV.admin.service.SystemLogService;
import com.CCM_EV.admin.service.SystemNotificationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

/**
 * Consumer for payment events from Payment service
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventConsumer {
    
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final SystemLogService logService;
    private final SystemNotificationService notificationService;
    private final AdminMetricsService metricsService;
    
    @RabbitListener(queues = "${app.rabbitmq.queues.payment-events:admin.payment.events}")
    @Transactional
    public void handlePaymentEvent(PaymentEventDTO event) {
        try {
            
            // Check if event already processed
            if (isEventProcessed(event.getEventId())) {
                log.debug("Event {} already processed, skipping", event.getEventId());
                return;
            }
            
            log.info("Processing payment event: {} for payment: {}", event.getStatus(), event.getPaymentId());
            
            // Upsert payment data
            upsertPayment(event);
            metricsService.recordPayment(event.getStatus());
            
            // Mark event as processed
            markEventAsProcessed(event.getEventId(), "PAYMENT_EVENT", event);
            
            // Log the event
            logService.createLog(
                "INFO",
                event.getSource(),
                "BUSINESS",
                "Payment event: " + event.getStatus(),
                objectMapper.writeValueAsString(event),
                event.getCorrelationId(),
                event.getPayerId()
            );
            
            // Create notification for failed payments
            if ("FAILED".equals(event.getStatus())) {
                notificationService.createNotification(
                    "WARNING",
                    "BUSINESS",
                    "Payment Failed",
                    String.format("Payment %s failed: %s", event.getPaymentId(), event.getErrorMessage()),
                    event.getSource(),
                    event.getPayerId(),
                    48 // expires in 48 hours
                );
            }
            
        } catch (Exception e) {
            log.error("Failed to process payment event", e);
            logService.createLog(
                "ERROR",
                "admin-service",
                "SYSTEM",
                "Failed to process payment event",
                e.getMessage(),
                null,
                null
            );
            metricsService.recordProcessingError();
        }
    }
    
    private void upsertPayment(PaymentEventDTO event) {
        var statusTimestamp = event.getCompletedAt() != null
                ? event.getCompletedAt()
                : (event.getFailedAt() != null ? event.getFailedAt() : event.getInitiatedAt());

        jdbc.update("""
            INSERT INTO fact_payment (
                payment_id, order_id, payer_id, payee_id, amount, currency, 
                status, payment_method, completed_at, region, status_changed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (payment_id, completed_at) DO UPDATE SET
                status = EXCLUDED.status,
                completed_at = EXCLUDED.completed_at,
                payer_id = EXCLUDED.payer_id,
                payee_id = EXCLUDED.payee_id,
                region = EXCLUDED.region,
                status_changed_at = EXCLUDED.status_changed_at
            """,
            event.getPaymentId(),
            event.getOrderId(),
            event.getPayerId(),
            event.getPayeeId(),
            event.getAmount(),
            event.getCurrency(),
            event.getStatus(),
            event.getPaymentMethod(),
            statusTimestamp,
            event.getRegion(),
            event.getTimestamp()
        );
    }
    
    private boolean isEventProcessed(String eventId) {
        Integer count = jdbc.queryForObject(
            "SELECT COUNT(*) FROM consumed_events WHERE event_id = ?",
            Integer.class,
            eventId
        );
        return count != null && count > 0;
    }
    
    private void markEventAsProcessed(String eventId, String eventType, PaymentEventDTO event) {
        try {
            jdbc.update("""
                INSERT INTO consumed_events (event_id, event_type, payload, received_at)
                VALUES (?, ?, ?::jsonb, ?)
                ON CONFLICT (event_id) DO NOTHING
                """,
                eventId,
                eventType,
                objectMapper.writeValueAsString(event),
                OffsetDateTime.now()
            );
        } catch (Exception e) {
            log.error("Failed to mark event as processed", e);
        }
    }
}
