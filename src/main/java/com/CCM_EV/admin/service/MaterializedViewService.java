package com.CCM_EV.admin.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

/**
 * Service to manage materialized view refreshes
 * Materialized views provide pre-aggregated data for fast reporting
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MaterializedViewService {
    
    private final JdbcTemplate jdbc;
    
    private static final String[] MATERIALIZED_VIEWS = {
        "mv_trades_daily",
        "mv_issuance_daily", 
        "mv_payments_daily",
        "mv_user_activity_daily"
    };
    
    /**
     * Refresh all materialized views
     * Called every hour to keep data fresh
     */
    @Scheduled(cron = "0 15 * * * *") // Every hour at :15
    @Transactional
    public void refreshAllViews() {
        log.info("Starting scheduled refresh of all materialized views");
        
        for (String viewName : MATERIALIZED_VIEWS) {
            refreshView(viewName);
        }
        
        log.info("Completed refresh of all materialized views");
    }
    
    /**
     * Refresh a single materialized view
     */
    @Transactional
    public void refreshView(String viewName) {
        OffsetDateTime startTime = OffsetDateTime.now();
        
        try {
            // Log start of refresh
            jdbc.update(
                "INSERT INTO mv_refresh_log (mv_name, refresh_started_at, status) VALUES (?, ?, 'RUNNING')",
                viewName, startTime
            );
            
            // Perform concurrent refresh (non-blocking for reads)
            jdbc.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY " + viewName);
            
            OffsetDateTime endTime = OffsetDateTime.now();
            
            // Get row count
            Long rowCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM " + viewName, 
                Long.class
            );
            
            // Log success
            jdbc.update(
                "UPDATE mv_refresh_log SET refresh_completed_at = ?, status = 'SUCCESS', rows_affected = ? " +
                "WHERE mv_name = ? AND refresh_started_at = ?",
                endTime, rowCount, viewName, startTime
            );
            
            log.info("Refreshed materialized view {} - {} rows in {} ms", 
                    viewName, rowCount, 
                    java.time.Duration.between(startTime, endTime).toMillis());
            
        } catch (Exception e) {
            log.error("Failed to refresh materialized view {}: {}", viewName, e.getMessage(), e);
            
            // Log failure
            jdbc.update(
                "UPDATE mv_refresh_log SET refresh_completed_at = ?, status = 'FAILED', error_message = ? " +
                "WHERE mv_name = ? AND refresh_started_at = ?",
                OffsetDateTime.now(), e.getMessage(), viewName, startTime
            );
        }
    }
    
    /**
     * Get last refresh time for a view
     */
    public OffsetDateTime getLastRefreshTime(String viewName) {
        try {
            return jdbc.queryForObject(
                "SELECT MAX(refresh_completed_at) FROM mv_refresh_log " +
                "WHERE mv_name = ? AND status = 'SUCCESS'",
                OffsetDateTime.class,
                viewName
            );
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Clean up old refresh logs (keep last 30 days)
     */
    @Scheduled(cron = "0 0 2 * * *") // Daily at 2 AM
    @Transactional
    public void cleanupOldLogs() {
        int deleted = jdbc.update(
            "DELETE FROM mv_refresh_log WHERE refresh_started_at < NOW() - INTERVAL '30 days'"
        );
        
        if (deleted > 0) {
            log.info("Cleaned up {} old materialized view refresh logs", deleted);
        }
    }
}
