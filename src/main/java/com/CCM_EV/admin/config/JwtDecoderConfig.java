package com.CCM_EV.admin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.web.client.RestOperations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Configuration
public class JwtDecoderConfig {

    @Bean
    @Primary
    JwtDecoder jwtDecoder(
            @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri:}") String jwksUri,
            @Value("${app.security.jwt.issuer}") String issuer,
            @Value("${app.security.jwt.audience}") String audience,
            @Value("${app.security.jwt.hmacSecret:}") String hsSecret,
            @Value("${app.security.jwt.clockSkewSec:60}") long skew,
            RestOperations jwtRestOps
    )
    {
        NimbusJwtDecoder dec;
        if (jwksUri != null && !jwksUri.isBlank()) { // JWKS
            var builder = NimbusJwtDecoder.withJwkSetUri(jwksUri)
                    .jwsAlgorithm(SignatureAlgorithm.RS256);

            if(jwtRestOps != null) {
                builder.restOperations(jwtRestOps);
            }
            dec = builder.build();

        } else if (hsSecret != null && !hsSecret.isBlank()) {
            SecretKey key = new SecretKeySpec(hsSecret.getBytes(StandardCharsets.UTF_8),"HmacSHA512");
            dec = NimbusJwtDecoder.withSecretKey(key).macAlgorithm(MacAlgorithm.HS512).build(); // HS512
        } else {
            throw new IllegalStateException("No JWKS or HS512 secret configured");
        }

        dec.setJwtValidator(new DelegatingOAuth2TokenValidator<>(new JwtTimestampValidator(Duration.ofSeconds(skew))));
        return dec;
    }

    @Bean
    RestOperations jwtRestOps(RestTemplateBuilder b) {
        Duration timeout = Duration.ofSeconds(2);
        return b.requestFactory(() -> {
            SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
            int millis = (int) timeout.toMillis();
            f.setConnectTimeout(millis);
            f.setReadTimeout(millis);
            return f;
        })
        .build();
    }
}
