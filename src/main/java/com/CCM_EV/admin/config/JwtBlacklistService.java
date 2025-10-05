package com.CCM_EV.admin.config;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JwtBlacklistService {
    private final StringRedisTemplate redis;

    @Value("${app.security.jwt.blacklistKeyPrefix}")
    private String prefix;

    public boolean isRevoked(String jti){
        if (jti==null || jti.isBlank()) return false;
        Boolean exists = redis.hasKey(prefix + jti);
        return Boolean.TRUE.equals(exists);
    }
}