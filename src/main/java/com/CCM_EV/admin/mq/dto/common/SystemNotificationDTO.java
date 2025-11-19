package com.CCM_EV.admin.mq.dto.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * System notification/alert events
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class SystemNotificationDTO extends BaseEvent {
    
    private String notificationId;
    private String level; // INFO, WARNING, ERROR, CRITICAL
    private String category; // SYSTEM, BUSINESS, SECURITY
    private String title;
    private String message;
    private String targetService;
    private String targetUserId; // null if broadcast
    private Boolean read;
}
