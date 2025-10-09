package com.CCM_EV.admin.mq.dto;

public record UserRegistered(
        String event_id, int schema_version, String occurred_at, String producer,
        Data data
){
    public record Data(
            long user_id,
            String username,
            String email,
            String role,
            String first_name,
            String last_name,
            String organization_name,
            String region,
            String created_at
    ){}
}
