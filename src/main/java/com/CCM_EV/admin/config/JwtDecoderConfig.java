package com.CCM_EV.admin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.*;

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
            @Value("${app.security.jwt.clockSkewSec:60}") long skew) {

        NimbusJwtDecoder dec;
        if (jwksUri != null && !jwksUri.isBlank()) {
            dec = NimbusJwtDecoder.withJwkSetUri(jwksUri).build();            // JWKS
        } else if (hsSecret != null && !hsSecret.isBlank()) {
            SecretKey key = new SecretKeySpec(hsSecret.getBytes(StandardCharsets.UTF_8),"HmacSHA512");
            dec = NimbusJwtDecoder.withSecretKey(key).macAlgorithm(MacAlgorithm.HS512).build(); // HS512
        } else {
            throw new IllegalStateException("No JWKS or HS512 secret configured");
        }

        var withIssuer = JwtValidators.createDefaultWithIssuer(issuer);
        OAuth2TokenValidator<Jwt> audienceValidator = jwt ->
                jwt.getAudience()!=null && jwt.getAudience().contains(audience)
                        ? OAuth2TokenValidatorResult.success()
                        : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token","bad audience",""));

        dec.setJwtValidator(new DelegatingOAuth2TokenValidator<>(withIssuer, audienceValidator, new JwtTimestampValidator(Duration.ofSeconds(skew))));
        return dec;
    }
}
