package com.CCM_EV.admin.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@RequiredArgsConstructor
class JwtRevocationFilter extends OncePerRequestFilter {
    private final JwtBlacklistService blacklist;

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
        var auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof JwtAuthenticationToken tok) {
            String jti = tok.getToken().getClaimAsString("jti");
            if (blacklist.isRevoked(jti)) {
                SecurityContextHolder.clearContext();
                res.setStatus(401);
                res.setContentType("application/json");
                res.getWriter().write("{\"error\":\"token_revoked\"}");
                return;
            }
        }
        chain.doFilter(req,res);
    }
}
