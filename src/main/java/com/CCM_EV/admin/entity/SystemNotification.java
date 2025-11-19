package com.CCM_EV.admin.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.OffsetDateTime;

/**
 * System notifications 
 */
@Entity
@Table(name = "system_notifications", indexes = {
    @Index(name = "idx_notifications_timestamp", columnList = "created_at DESC"),
    @Index(name = "idx_notifications_level", columnList = "level"),
    @Index(name = "idx_notifications_target", columnList = "target_user_id, read_status"),
    @Index(name = "idx_notifications_category", columnList = "category")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SystemNotification {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "notification_id", unique = true, nullable = false, length = 100)
    private String notificationId;
    
    @Column(name = "level", nullable = false, length = 20)
    private String level; // INFO, WARNING, ERROR, CRITICAL
    
    @Column(name = "category", nullable = false, length = 50)
    private String category; // SYSTEM, BUSINESS, SECURITY
    
    @Column(name = "title", nullable = false, length = 255)
    private String title;
    
    @Column(name = "message", columnDefinition = "TEXT")
    private String message;
    
    @Column(name = "source_service", length = 50)
    private String sourceService;
    
    @Column(name = "target_service", length = 50)
    private String targetService;
    
    @Column(name = "target_user_id", length = 50)
    private String targetUserId; // null if broadcast
    
    @Column(name = "read_status", nullable = false)
    @Builder.Default
    private Boolean readStatus = false;
    
    @Column(name = "read_at")
    private OffsetDateTime readAt;
    
    @Column(name = "created_at", nullable = false)
    @CreationTimestamp
    private OffsetDateTime createdAt;
    
    @Column(name = "expires_at")
    private OffsetDateTime expiresAt;
}
