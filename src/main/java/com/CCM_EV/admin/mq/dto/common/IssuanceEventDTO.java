package com.CCM_EV.admin.mq.dto.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * Carbon credit issuance events from CarbonModule
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class IssuanceEventDTO extends BaseEvent {
    
    private String issuanceId;  
    private String requestId;   
    private String vehicleId;   
    private BigDecimal quantityTco2e;
    private BigDecimal distanceKm;
    private BigDecimal energyKwh;
    private BigDecimal co2AvoidedKg;
    private String status; // PENDING, APPROVED, REJECTED
    private String region;
    
    // Vehicle info for reporting
    private String vehicleMake;
    private String vehicleModel;
    private String vehicleType;
}
