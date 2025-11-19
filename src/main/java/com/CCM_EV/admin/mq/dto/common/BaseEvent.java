package com.CCM_EV.admin.mq.dto.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.OffsetDateTime;

/**
 * Base event DTO 
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseEvent {
    
    /**
     * Unique event ID 
     */
    private String eventId;
    
    /**
     * Event type 
     */
    private String eventType;
    
    /**
     * Source service name
     */
    private String source;
    
    /**
     * Event timestamp
     */
    private OffsetDateTime timestamp;
    
    /**
     * Event version 
     */
    private String version;
    
    /**
     * Correlation ID 
     */
    private String correlationId;
    
    /**
     * User ID who triggered this event (if applicable)
     */
    private String userId;
}
