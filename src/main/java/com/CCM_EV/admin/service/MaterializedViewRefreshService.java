package com.CCM_EV.admin.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Service for automatic materialized view refresh
 * Refreshes MVs on a schedule to keep reporting data up-to-date
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MaterializedViewRefreshService {

    private final JdbcTemplate jdbcTemplate;
    
    private static final List<String> MATERIALIZED_VIEWS = Arrays.asList(
        "mv_trades_daily",
        "mv_issuance_daily",
        "mv_payments_daily",
        "mv_user_activity_daily"
    );
    
    /**
     * Refresh all materialized views
     * Runs every hour at minute 5
     */
    @Scheduled(cron = "0 5 * * * ?")
    public void refreshAllViews() {
        log.info("Starting scheduled refresh of all materialized views");
        
        for (String viewName : MATERIALIZED_VIEWS) {
            refreshView(viewName, false);
        }
        
        log.info("Completed scheduled refresh of all materialized views");
    }
    
    /**
     * Refresh a specific materialized view
     * 
     * @param viewName Name of the materialized view
     * @param concurrent If true, uses CONCURRENTLY (doesn't block queries but slower)
     */
    @Transactional
    public void refreshView(String viewName, boolean concurrent) {
        Instant startTime = Instant.now();
        Long logId = null;
        
        try {
            logId = jdbcTemplate.queryForObject(
                """
                INSERT INTO mv_refresh_log (mv_name, refresh_started_at, status)
                VALUES (?, ?, 'RUNNING')
                RETURNING id
                """,
                Long.class,
                viewName,
                java.sql.Timestamp.from(startTime)
            );

            String refreshSql = concurrent 
                ? String.format("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", viewName)
                : String.format("REFRESH MATERIALIZED VIEW %s", viewName);
            
            jdbcTemplate.execute(refreshSql);

            Long rowCount = jdbcTemplate.queryForObject(
                String.format("SELECT count(*) FROM %s", viewName),
                Long.class
            );
            
            Instant endTime = Instant.now();
            long durationMs = endTime.toEpochMilli() - startTime.toEpochMilli();

            jdbcTemplate.update(
                """
                UPDATE mv_refresh_log 
                SET refresh_completed_at = ?,
                    status = 'SUCCESS',
                    rows_affected = ?
                WHERE id = ?
                """,
                java.sql.Timestamp.from(endTime),
                rowCount,
                logId
            );
            
            log.info("Successfully refreshed materialized view: {} in {}ms, rows: {}", 
                     viewName, durationMs, rowCount);
            
        } catch (Exception e) {
            log.error("Error refreshing materialized view: {}", viewName, e);
            
            if (logId != null) {
                jdbcTemplate.update(
                    """
                    UPDATE mv_refresh_log 
                    SET refresh_completed_at = ?,
                        status = 'FAILED',
                        error_message = ?
                    WHERE id = ?
                    """,
                    java.sql.Timestamp.from(Instant.now()),
                    e.getMessage(),
                    logId
                );
            }
            
            throw new RuntimeException("Failed to refresh materialized view: " + viewName, e);
        }
    }
    
    /**
     * Refresh all views concurrently (doesn't block queries)
     * Useful for production environments
     * Runs daily at 4 AM
     */
    @Scheduled(cron = "0 0 4 * * ?")
    public void refreshAllViewsConcurrently() {
        log.info("Starting concurrent refresh of all materialized views");
        
        for (String viewName : MATERIALIZED_VIEWS) {
            try {
                refreshView(viewName, true);
            } catch (Exception e) {
                log.error("Error in concurrent refresh of {}, continuing with next view", viewName, e);
                // Continue with other views even if one fails
            }
        }
        
        log.info("Completed concurrent refresh of all materialized views");
    }
    
    /**
     * Get refresh history for a specific view
     */
    public List<Map<String, Object>> getRefreshHistory(String viewName, int limit) {
        String sql = """
            SELECT 
                mv_name,
                refresh_started_at,
                refresh_completed_at,
                status,
                rows_affected,
                EXTRACT(EPOCH FROM (refresh_completed_at - refresh_started_at)) as duration_seconds,
                error_message
            FROM mv_refresh_log
            WHERE mv_name = ?
            ORDER BY refresh_started_at DESC
            LIMIT ?
            """;
        
        return jdbcTemplate.queryForList(sql, viewName, limit);
    }
    
    /**
     * Get all refresh history
     */
    public List<Map<String, Object>> getAllRefreshHistory(int limit) {
        String sql = """
            SELECT 
                mv_name,
                refresh_started_at,
                refresh_completed_at,
                status,
                rows_affected,
                EXTRACT(EPOCH FROM (refresh_completed_at - refresh_started_at)) as duration_seconds
            FROM mv_refresh_log
            ORDER BY refresh_started_at DESC
            LIMIT ?
            """;
        
        return jdbcTemplate.queryForList(sql, limit);
    }
    
    /**
     * Get materialized view statistics
     */
    public List<Map<String, Object>> getViewStatistics() {
        String sql = """
            SELECT 
                schemaname,
                matviewname as view_name,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size,
                (SELECT count(*) FROM pg_class WHERE relname = matviewname) as exists
            FROM pg_matviews
            WHERE schemaname = 'public'
            AND matviewname LIKE 'mv_%'
            ORDER BY matviewname
            """;
        
        return jdbcTemplate.queryForList(sql);
    }
    
    /**
     * Check if materialized views need refresh
     * Returns views that haven't been refreshed in the last hour
     */
    public List<String> getStaleViews() {
        String sql = """
            WITH latest_refresh AS (
                SELECT DISTINCT ON (mv_name) 
                    mv_name,
                    refresh_completed_at,
                    status
                FROM mv_refresh_log
                ORDER BY mv_name, refresh_started_at DESC
            )
            SELECT unnest(ARRAY[?]) as mv_name
            WHERE NOT EXISTS (
                SELECT 1 FROM latest_refresh 
                WHERE latest_refresh.mv_name = unnest(ARRAY[?])
                AND status = 'SUCCESS'
                AND refresh_completed_at > now() - interval '1 hour'
            )
            """;
        
        String[] views = MATERIALIZED_VIEWS.toArray(new String[0]);
        return jdbcTemplate.queryForList(sql, String.class, views, views);
    }
    
    /**
     * Force refresh of stale views
     */
    public void refreshStaleViews() {
        List<String> staleViews = getStaleViews();
        
        if (staleViews.isEmpty()) {
            log.info("No stale views to refresh");
            return;
        }
        
        log.info("Refreshing {} stale views: {}", staleViews.size(), staleViews);
        
        for (String viewName : staleViews) {
            try {
                refreshView(viewName, false);
            } catch (Exception e) {
                log.error("Error refreshing stale view: {}", viewName, e);
            }
        }
    }
    
    /**
     * Clean up old refresh logs (keep last 100 per view)
     */
    @Scheduled(cron = "0 0 5 * * SUN") // Weekly on Sunday at 5 AM
    @Transactional
    public void cleanupOldLogs() {
        log.info("Starting cleanup of old MV refresh logs");
        
        String sql = """
            DELETE FROM mv_refresh_log
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY mv_name ORDER BY refresh_started_at DESC) as rn
                    FROM mv_refresh_log
                ) sub
                WHERE rn > 100
            )
            """;
        
        int deleted = jdbcTemplate.update(sql);
        log.info("Cleaned up {} old MV refresh log entries", deleted);
    }
}
