package com.CCM_EV.admin.mq.dto;

public record TradeExecuted(
        String event_id, int schema_version, String occurred_at, String producer,
        Data data
){
    public record Data(
            long order_id, long buyer_id, long seller_id, Long listing_id,
            double quantity, String unit, double unit_price, double amount, String currency, String executed_at
    ){}
}
