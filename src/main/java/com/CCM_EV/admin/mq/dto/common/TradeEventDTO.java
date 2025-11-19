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
 * Trade/Order events from Marketplace service
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TradeEventDTO extends BaseEvent {
    
    private UUID orderId;
    private UUID listingId;
    private String buyerId;
    private String sellerId;
    private BigDecimal quantity;
    private String quantityUnit;
    private BigDecimal unitPrice;
    private BigDecimal amount;
    private String currency;
    private String orderStatus; // CREATED, UPDATED, COMPLETED, CANCELLED
    private String region; // Region of the seller or buyer
    private OffsetDateTime statusChangedAt;
    
    // Auction-specific
    private Boolean isAuction;
    private UUID auctionId;
}
