package com.CCM_EV.admin.mq.dto.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Payment events from Payment service
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class PaymentEventDTO extends BaseEvent {
    
    private String paymentId;
    private String orderId; // Changed from UUID to String to support non-UUID order IDs
    private String payerId;
    private String payeeId;
    private BigDecimal amount;
    private String currency;
    private String status; // PENDING, PROCESSING, COMPLETED, FAILED, REFUNDED
    private String paymentMethod;
    private String region;
    private OffsetDateTime initiatedAt;
    private OffsetDateTime completedAt;
    private OffsetDateTime failedAt;
    
    // Error info if failed
    private String errorCode;
    private String errorMessage;
}
