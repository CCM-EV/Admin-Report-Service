package com.CCM_EV.admin.mq.dto.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * User-related events (register, login, update, delete)
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class UserEventDTO extends BaseEvent {
    
    private String username;
    private String email;
    private String role;
    private String region;
    private Boolean enabled;
    private String action; // REGISTERED, LOGGED_IN, UPDATED, DELETED, ENABLED, DISABLED
    
    // Optional metadata
    private String ipAddress;
    private String userAgent;
    private String organizationName;
    private String vehicleMake;
    private String vehicleModel;
    private String phoneNumber;
}
