package com.CCM_EV.admin.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.OffsetDateTime;

/**
 * System logs 
 */
@Entity
@Table(name = "system_logs", indexes = {
    @Index(name = "idx_system_logs_timestamp", columnList = "log_timestamp DESC"),
    @Index(name = "idx_system_logs_level", columnList = "log_level"),
    @Index(name = "idx_system_logs_source", columnList = "source_service"),
    @Index(name = "idx_system_logs_correlation", columnList = "correlation_id")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SystemLog {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "log_level", nullable = false, length = 20)
    private String logLevel; // DEBUG, INFO, WARN, ERROR, FATAL
    
    @Column(name = "source_service", nullable = false, length = 50)
    private String sourceService;
    
    @Column(name = "category", length = 50)
    private String category; // SYSTEM, BUSINESS, SECURITY, PERFORMANCE
    
    @Column(name = "message", columnDefinition = "TEXT")
    private String message;
    
    @Column(name = "details", columnDefinition = "TEXT")
    private String details; 
    
    @Column(name = "correlation_id", length = 100)
    private String correlationId;
    
    @Column(name = "user_id")
    private String userId;
    
    @Column(name = "ip_address", length = 50)
    private String ipAddress;
    
    @Column(name = "log_timestamp", nullable = false)
    @CreationTimestamp
    private OffsetDateTime logTimestamp;
}
