package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.metrics.AdminMetricsService;
import com.CCM_EV.admin.mq.dto.common.IssuanceEventDTO;
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
 * Consumer for carbon credit issuance events from CarbonModule
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IssuanceEventConsumer {
    
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final SystemLogService logService;
    private final SystemNotificationService notificationService;
    private final AdminMetricsService metricsService;
    
    @RabbitListener(queues = "${app.rabbitmq.queues.issuance-events:admin.issuance.events}")
    @Transactional
    public void handleIssuanceEvent(IssuanceEventDTO event) {
        try {
            
            // Check if event already processed
            if (isEventProcessed(event.getEventId())) {
                log.debug("Event {} already processed, skipping", event.getEventId());
                return;
            }
            
            log.info("Processing issuance event: {} for request: {}", event.getStatus(), event.getRequestId());
            
            // Process all issuance statuses
            upsertIssuance(event);
            
            // Create appropriate notifications based on status
            switch (event.getStatus()) {
                case "PENDING":
                    notificationService.createNotification(
                        "INFO",
                        "BUSINESS",
                        "Carbon Credit Request Submitted",
                        String.format("Your carbon credit request for %.2f tCO2e is being reviewed", event.getQuantityTco2e()),
                        event.getSource(),
                        String.valueOf(event.getUserId()),
                        168 // expires in 7 days
                    );
                    break;
                    
                case "APPROVED":
                    metricsService.recordCreditIssued();
                    notificationService.createNotification(
                        "SUCCESS",
                        "BUSINESS",
                        "Carbon Credits Issued",
                        String.format("%.2f tCO2e carbon credits have been issued to your account", event.getQuantityTco2e()),
                        event.getSource(),
                        String.valueOf(event.getUserId()),
                        168 // expires in 7 days
                    );
                    break;
                    
                case "REJECTED":
                    notificationService.createNotification(
                        "WARNING",
                        "BUSINESS",
                        "Carbon Credit Request Rejected",
                        String.format("Your carbon credit request for %.2f tCO2e has been rejected", event.getQuantityTco2e()),
                        event.getSource(),
                        String.valueOf(event.getUserId()),
                        168 // expires in 7 days
                    );
                    break;
            }
            
            // Mark event as processed
            markEventAsProcessed(event.getEventId(), "ISSUANCE_EVENT", event);
            
            // Log the event
            logService.createLog(
                "INFO",
                event.getSource(),
                "BUSINESS",
                "Issuance event: " + event.getStatus(),
                objectMapper.writeValueAsString(event),
                event.getCorrelationId(),
                String.valueOf(event.getUserId())
            );
            
        } catch (Exception e) {
            log.error("Failed to process issuance event", e);
            logService.createLog(
                "ERROR",
                "admin-service",
                "SYSTEM",
                "Failed to process issuance event",
                e.getMessage(),
                null,
                null
            );
            metricsService.recordProcessingError();
        }
    }
    
    private void upsertIssuance(IssuanceEventDTO event) {
        // Convert userId from String to Long for database
        Long userId = null;
        try {
            if (event.getUserId() != null && !event.getUserId().isEmpty()) {
                userId = Long.parseLong(event.getUserId());
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid userId format: {}", event.getUserId());
        }
        
        // Use requestId as unique identifier (issuanceId is null for PENDING/REJECTED)
        String uniqueId = event.getIssuanceId() != null ? event.getIssuanceId() : event.getRequestId();
        
        jdbc.update("""
            INSERT INTO fact_issuance (
                issuance_id, user_id, vehicle_id, quantity_tco2e, distance_km, 
                energy_kwh, co2_avoided_kg, issued_at, region, request_id, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (issuance_id, issued_at) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                vehicle_id = EXCLUDED.vehicle_id,
                quantity_tco2e = EXCLUDED.quantity_tco2e,
                distance_km = EXCLUDED.distance_km,
                energy_kwh = EXCLUDED.energy_kwh,
                co2_avoided_kg = EXCLUDED.co2_avoided_kg,
                region = EXCLUDED.region,
                status = EXCLUDED.status
            """,
            uniqueId,
            userId,
            event.getVehicleId(),
            event.getQuantityTco2e(),
            event.getDistanceKm(),
            event.getEnergyKwh(),
            event.getCo2AvoidedKg(),
            event.getTimestamp(),
            event.getRegion(),
            event.getRequestId(),
            event.getStatus()
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
    
    private void markEventAsProcessed(String eventId, String eventType, IssuanceEventDTO event) {
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
