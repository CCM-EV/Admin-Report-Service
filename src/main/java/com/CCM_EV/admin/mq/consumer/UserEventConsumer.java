package com.CCM_EV.admin.mq.consumer;

import com.CCM_EV.admin.metrics.AdminMetricsService;
import com.CCM_EV.admin.mq.dto.common.UserEventDTO;
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
 * Consumer for user-related events (register, login, update, etc.)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UserEventConsumer {
    
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final SystemLogService logService;
    private final SystemNotificationService notificationService;
    private final AdminMetricsService metricsService;
    
    @RabbitListener(queues = "${app.rabbitmq.queues.user-events:admin.user.events}")
    @Transactional
    public void handleUserEvent(UserEventDTO event) {
        try {
            
            // Check if event already processed (deduplication)
            if (isEventProcessed(event.getEventId())) {
                log.debug("Event {} already processed, skipping", event.getEventId());
                return;
            }
            
            log.info("Processing user event: {} for user: {}", event.getAction(), event.getUserId());
            
            // Process based on action type
            switch (event.getAction()) {
                case "REGISTERED":
                    handleUserRegistered(event);
                    metricsService.recordUserRegistered();
                    break;
                case "LOGGED_IN":
                    handleUserLoggedIn(event);
                    metricsService.recordUserLogin();
                    break;
                case "UPDATED":
                    handleUserUpdated(event);
                    break;
                case "DELETED":
                    handleUserDeleted(event);
                    break;
                case "ENABLED":
                case "DISABLED":
                    handleUserStatusChanged(event);
                    break;
                default:
                    log.warn("Unknown user action: {}", event.getAction());
            }
            
            // Mark event as processed
            markEventAsProcessed(event.getEventId(), "USER_EVENT", event);
            
            // Log the event
            logService.createLog(
                "INFO",
                event.getSource(),
                "USER",
                "User event: " + event.getAction(),
                objectMapper.writeValueAsString(event),
                event.getCorrelationId(),
                String.valueOf(event.getUserId())
            );
            
        } catch (Exception e) {
            log.error("Failed to process user event", e);
            logService.createLog(
                "ERROR",
                "admin-service",
                "SYSTEM",
                "Failed to process user event",
                e.getMessage(),
                null,
                null
            );
            metricsService.recordProcessingError();
        }
    }
    
    private void handleUserRegistered(UserEventDTO event) {
        // Upsert to dim_users
        jdbc.update("""
            INSERT INTO dim_users (user_id, username, email, role, region, enabled, created_at, updated_at, organization_name, phone_number, last_login_at)
            VALUES (CAST(? AS bigint), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                email = EXCLUDED.email,
                role = EXCLUDED.role,
                region = EXCLUDED.region,
                enabled = EXCLUDED.enabled,
                updated_at = EXCLUDED.updated_at,
                organization_name = EXCLUDED.organization_name,
                phone_number = EXCLUDED.phone_number,
                last_login_at = EXCLUDED.last_login_at
            """,
            event.getUserId(),
            event.getUsername(),
            event.getEmail(),
            event.getRole(),
            event.getRegion(),
            event.getEnabled() != null ? event.getEnabled() : true,
            event.getTimestamp(),
            event.getTimestamp(),
            event.getOrganizationName(),
            event.getPhoneNumber(),
            event.getTimestamp()
        );
        
        // Record activity
        recordUserActivity(event, "REGISTERED");
        
        // Create notification for admin
        notificationService.createNotification(
            "INFO",
            "BUSINESS",
            "New User Registered",
            String.format("User %s registered with role %s", event.getUsername(), event.getRole()),
            event.getSource(),
            null, // broadcast to admins
            72 // expires in 72 hours
        );
    }
    
    private void handleUserLoggedIn(UserEventDTO event) {
        // Update last login
        jdbc.update("""
            UPDATE dim_users 
            SET last_login_at = ?, updated_at = ?
            WHERE user_id = CAST(? AS bigint)
            """,
            event.getTimestamp(),
            event.getTimestamp(),
            event.getUserId()
        );
        
        // Record activity
        recordUserActivity(event, "LOGGED_IN");
    }
    
    private void handleUserUpdated(UserEventDTO event) {
        // Update user info
        jdbc.update("""
            UPDATE dim_users 
            SET username = COALESCE(?, username),
                email = COALESCE(?, email),
                role = COALESCE(?, role),
                region = COALESCE(?, region),
                organization_name = COALESCE(?, organization_name),
                phone_number = COALESCE(?, phone_number),
                updated_at = ?
            WHERE user_id = CAST(? AS bigint)
            """,
            event.getUsername(),
            event.getEmail(),
            event.getRole(),
            event.getRegion(),
            event.getOrganizationName(),
            event.getPhoneNumber(),
            event.getTimestamp(),
            event.getUserId()
        );
        
        // Record activity
        recordUserActivity(event, "UPDATED");
    }
    
    private void handleUserDeleted(UserEventDTO event) {
        // Soft delete or mark as deleted
        jdbc.update("""
            UPDATE dim_users 
            SET enabled = false, updated_at = ?
            WHERE user_id = CAST(? AS bigint)
            """,
            event.getTimestamp(),
            event.getUserId()
        );
        
        // Record activity
        recordUserActivity(event, "DELETED");
    }
    
    private void handleUserStatusChanged(UserEventDTO event) {
        jdbc.update("""
            UPDATE dim_users 
            SET enabled = ?, updated_at = ?
            WHERE user_id = CAST(? AS bigint)
            """,
            event.getEnabled(),
            event.getTimestamp(),
            event.getUserId()
        );
        
        // Record activity
        recordUserActivity(event, event.getAction());
    }
    
    private void recordUserActivity(UserEventDTO event, String eventType) {
        try {
            String eventData = objectMapper.writeValueAsString(event);
            jdbc.update("""
                INSERT INTO fact_user_activity (user_id, event_type, event_data, occurred_at)
                VALUES (CAST(? AS bigint), ?, ?::jsonb, ?)
                """,
                event.getUserId(),
                eventType,
                eventData,
                event.getTimestamp()
            );
        } catch (Exception e) {
            log.error("Failed to record user activity", e);
        }
    }
    
    private boolean isEventProcessed(String eventId) {
        Integer count = jdbc.queryForObject(
            "SELECT COUNT(*) FROM consumed_events WHERE event_id = ?",
            Integer.class,
            eventId
        );
        return count != null && count > 0;
    }
    
    private void markEventAsProcessed(String eventId, String eventType, UserEventDTO event) {
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
