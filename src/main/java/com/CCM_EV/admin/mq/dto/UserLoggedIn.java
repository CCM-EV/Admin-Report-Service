package com.CCM_EV.admin.mq.dto;

public record UserLoggedIn(
        String event_id, int schema_version, String occurred_at, String producer,
        Data data
){
    public record Data(
            long user_id,
            String username,
            String ip_address,
            String user_agent,
            String login_at
    ){}
}
