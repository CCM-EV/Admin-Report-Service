package com.CCM_EV.admin.mq.dto;

public record CreditIssued(
        String event_id, int schema_version, String occurred_at, String producer,
        Data data
){
    public record Data(
            String issuance_id,
            long user_id,
            String request_id,
            double quantity_tco2e,
            Double distance_km,
            Double energy_kwh,
            Double co2_avoided_kg,
            String issued_at
    ){}
}
