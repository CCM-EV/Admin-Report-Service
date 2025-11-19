package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.metrics.AdminMetricsService;
import com.CCM_EV.admin.mq.dto.common.TradeEventDTO;
import com.CCM_EV.admin.service.SystemLogService;
import com.CCM_EV.admin.service.SystemNotificationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * Consumer for trade/order events from Marketplace service
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TradeEventConsumer {
    
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final SystemLogService logService;
    private final SystemNotificationService notificationService;
    private final AdminMetricsService metricsService;
    
    @RabbitListener(queues = "${app.rabbitmq.queues.trade-events:admin.trade.events}")
    @Transactional
    public void handleTradeEvent(TradeEventDTO event) {
        try {
            
            // Check if event already processed
            if (isEventProcessed(event.getEventId())) {
                log.debug("Event {} already processed, skipping", event.getEventId());
                return;
            }
            
            log.info("Processing trade event: {} for order: {}", event.getEventType(), event.getOrderId());
            
            // Upsert trade data
            upsertTrade(event);
            if ("COMPLETED".equalsIgnoreCase(event.getOrderStatus()) ||
                "PENDING_PAYMENT".equalsIgnoreCase(event.getOrderStatus())) {
                metricsService.recordTradeExecuted();
            }
            
            // Mark event as processed
            markEventAsProcessed(event.getEventId(), "TRADE_EVENT", event);
            
            // Log the event
            logService.createLog(
                "INFO",
                event.getSource(),
                "BUSINESS",
                "Trade event: " + event.getOrderStatus(),
                objectMapper.writeValueAsString(event),
                event.getCorrelationId(),
                event.getBuyerId()
            );
            
            // Create notification for high-value trades
            if (event.getAmount() != null && event.getAmount().compareTo(new BigDecimal("1000000")) > 0) {
                notificationService.createNotification(
                    "INFO",
                    "BUSINESS",
                    "High-Value Trade Executed",
                    String.format("Trade of %s %s executed", event.getAmount(), event.getCurrency()),
                    event.getSource(),
                    null, // broadcast
                    24
                );
            }
            
        } catch (Exception e) {
            log.error("Failed to process trade event", e);
            logService.createLog(
                "ERROR",
                "admin-service",
                "SYSTEM",
                "Failed to process trade event",
                e.getMessage(),
                null,
                null
            );
            metricsService.recordProcessingError();
        }
    }
    
    private void upsertTrade(TradeEventDTO event) {
        // Convert buyerId and sellerId from String to Long for database
        Long buyerId = null;
        Long sellerId = null;
        try {
            if (event.getBuyerId() != null && !event.getBuyerId().isEmpty()) {
                buyerId = Long.parseLong(event.getBuyerId());
            }
            if (event.getSellerId() != null && !event.getSellerId().isEmpty()) {
                sellerId = Long.parseLong(event.getSellerId());
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid buyerId or sellerId format: buyer={}, seller={}", 
                    event.getBuyerId(), event.getSellerId());
        }
        
        jdbc.update("""
            INSERT INTO fact_trade (
                order_id, listing_id, buyer_id, seller_id, quantity, unit, unit_price, 
                amount, currency, executed_at, region, is_auction, order_status, status_changed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id, executed_at) DO UPDATE SET
                listing_id = EXCLUDED.listing_id,
                buyer_id = EXCLUDED.buyer_id,
                seller_id = EXCLUDED.seller_id,
                quantity = EXCLUDED.quantity,
                unit = EXCLUDED.unit,
                unit_price = EXCLUDED.unit_price,
                amount = EXCLUDED.amount,
                currency = EXCLUDED.currency,
                region = EXCLUDED.region,
                is_auction = EXCLUDED.is_auction,
                order_status = EXCLUDED.order_status,
                status_changed_at = EXCLUDED.status_changed_at
            """,
            event.getOrderId(),
            event.getListingId(),
            buyerId,
            sellerId,
            event.getQuantity(),
            event.getQuantityUnit() != null ? event.getQuantityUnit() : "tCO2e",
            event.getUnitPrice(),
            event.getAmount(),
            event.getCurrency(),
            event.getTimestamp(),
            event.getRegion(),
            event.getIsAuction() != null ? event.getIsAuction() : false,
            event.getOrderStatus(),
            event.getStatusChangedAt() != null ? event.getStatusChangedAt() : OffsetDateTime.now()
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
    
    private void markEventAsProcessed(String eventId, String eventType, TradeEventDTO event) {
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
