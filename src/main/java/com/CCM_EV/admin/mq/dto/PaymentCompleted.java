package com.CCM_EV.admin.mq.dto;

public record PaymentCompleted(
        String event_id, int schema_version, String occurred_at, String producer,
        Data data
){
    public record Data(
            String payment_id,
            String order_id,
            double amount,
            String currency,
            String status,
            String payment_method,
            String completed_at
    ){}
}
