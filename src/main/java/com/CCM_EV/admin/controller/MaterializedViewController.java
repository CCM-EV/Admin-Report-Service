package com.CCM_EV.admin.controller;

import com.CCM_EV.admin.service.MaterializedViewService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Controller for managing materialized views
 * Provides endpoints to refresh views and check their status
 */
@RestController
@RequestMapping("/api/admin/materialized-views")
@RequiredArgsConstructor
@Tag(name = "Materialized Views", description = "Manage and monitor materialized views")
@SecurityRequirement(name = "Bearer Authentication")
@PreAuthorize("hasRole('ADMIN')")
public class MaterializedViewController {
    
    private final MaterializedViewService mvService;
    private final JdbcTemplate jdbc;
    
    @PostMapping("/{viewName}/refresh")
    @Operation(summary = "Manually refresh a materialized view")
    public Map<String, Object> refreshView(@PathVariable String viewName) {
        OffsetDateTime startTime = OffsetDateTime.now();
        mvService.refreshView(viewName);
        OffsetDateTime endTime = OffsetDateTime.now();
        
        return Map.of(
            "viewName", viewName,
            "refreshedAt", endTime,
            "durationMs", java.time.Duration.between(startTime, endTime).toMillis(),
            "status", "SUCCESS"
        );
    }
    
    @PostMapping("/refresh-all")
    @Operation(summary = "Refresh all materialized views")
    public Map<String, Object> refreshAllViews() {
        OffsetDateTime startTime = OffsetDateTime.now();
        mvService.refreshAllViews();
        OffsetDateTime endTime = OffsetDateTime.now();
        
        return Map.of(
            "refreshedAt", endTime,
            "durationMs", java.time.Duration.between(startTime, endTime).toMillis(),
            "status", "SUCCESS",
            "message", "All materialized views refreshed"
        );
    }
    
    @GetMapping("/status")
    @Operation(summary = "Get status of all materialized views")
    public Map<String, Object> getStatus() {
        Map<String, Object> result = new HashMap<>();
        
        String[] views = {
            "mv_trades_daily",
            "mv_issuance_daily",
            "mv_payments_daily",
            "mv_user_activity_daily"
        };
        
        List<Map<String, Object>> viewStatus = new java.util.ArrayList<>();
        
        for (String viewName : views) {
            Map<String, Object> status = new HashMap<>();
            status.put("view_name", viewName);
            status.put("viewName", viewName);
            
            // Get row count
            Long rowCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM " + viewName,
                Long.class
            );
            status.put("row_count", rowCount);
            status.put("rowCount", rowCount);
            
            // Get size
            String size = jdbc.queryForObject(
                "SELECT pg_size_pretty(pg_total_relation_size(?::regclass))",
                String.class,
                viewName
            );
            status.put("size", size);
            
            // Get last refresh time
            OffsetDateTime lastRefresh = mvService.getLastRefreshTime(viewName);
            status.put("last_refresh", lastRefresh);
            status.put("lastRefresh", lastRefresh);
            
            // Get data freshness
            if (lastRefresh != null) {
                long hoursSinceRefresh = java.time.Duration.between(
                    lastRefresh, 
                    OffsetDateTime.now()
                ).toHours();
                status.put("hoursSinceRefresh", hoursSinceRefresh);
                status.put("isStale", hoursSinceRefresh > 2); // Stale if > 2 hours
            }
            
            viewStatus.add(status);
        }
        
        result.put("views", viewStatus);
        result.put("checkedAt", OffsetDateTime.now());
        
        return result;
    }
    
    @GetMapping("/refresh-history")
    @Operation(summary = "Get refresh history for all views")
    public Map<String, Object> getRefreshHistory(
        @RequestParam(defaultValue = "50") int limit
    ) {
        List<Map<String, Object>> history = jdbc.queryForList(
            "SELECT mv_name, refresh_started_at, refresh_completed_at, status, " +
            "       rows_affected, error_message, " +
            "       EXTRACT(EPOCH FROM (refresh_completed_at - refresh_started_at)) * 1000 as duration_ms " +
            "FROM mv_refresh_log " +
            "ORDER BY refresh_started_at DESC " +
            "LIMIT ?",
            limit
        );
        
        return Map.of(
            "history", history,
            "limit", limit,
            "retrievedAt", OffsetDateTime.now()
        );
    }
    
    @GetMapping("/{viewName}/refresh-history")
    @Operation(summary = "Get refresh history for a specific view")
    public Map<String, Object> getViewRefreshHistory(
        @PathVariable String viewName,
        @RequestParam(defaultValue = "20") int limit
    ) {
        List<Map<String, Object>> history = jdbc.queryForList(
            "SELECT refresh_started_at, refresh_completed_at, status, " +
            "       rows_affected, error_message, " +
            "       EXTRACT(EPOCH FROM (refresh_completed_at - refresh_started_at)) * 1000 as duration_ms " +
            "FROM mv_refresh_log " +
            "WHERE mv_name = ? " +
            "ORDER BY refresh_started_at DESC " +
            "LIMIT ?",
            viewName, limit
        );
        
        // Get statistics
        Map<String, Object> stats = jdbc.queryForMap(
            "SELECT " +
            "   COUNT(*) as total_refreshes, " +
            "   SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful, " +
            "   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed, " +
            "   AVG(EXTRACT(EPOCH FROM (refresh_completed_at - refresh_started_at)) * 1000) as avg_duration_ms " +
            "FROM mv_refresh_log " +
            "WHERE mv_name = ? AND refresh_started_at > NOW() - INTERVAL '7 days'",
            viewName
        );
        
        return Map.of(
            "viewName", viewName,
            "history", history,
            "statistics", stats,
            "retrievedAt", OffsetDateTime.now()
        );
    }
    
    @GetMapping("/performance-comparison")
    @Operation(summary = "Compare performance of MV queries vs direct queries")
    public Map<String, Object> getPerformanceComparison() {
        Map<String, Object> result = new HashMap<>();
        
        // Test query performance - MV vs direct
        
        // 1. Trade stats - MV query
        long mvStart = System.currentTimeMillis();
        jdbc.queryForList(
            "SELECT day, trade_count, revenue FROM mv_trades_daily " +
            "WHERE day > NOW() - INTERVAL '30 days' ORDER BY day DESC"
        );
        long mvDuration = System.currentTimeMillis() - mvStart;
        
        // 2. Trade stats - Direct query
        long directStart = System.currentTimeMillis();
        jdbc.queryForList(
            "SELECT DATE(executed_at) as day, COUNT(*) as trade_count, SUM(amount) as revenue " +
            "FROM fact_trade " +
            "WHERE executed_at > NOW() - INTERVAL '30 days' " +
            "GROUP BY DATE(executed_at) ORDER BY day DESC"
        );
        long directDuration = System.currentTimeMillis() - directStart;
        
        result.put("materialized_view_ms", mvDuration);
        result.put("direct_query_ms", directDuration);
        result.put("performance_improvement", String.format("%.1fx faster", (double) directDuration / mvDuration));
        result.put("time_saved_ms", directDuration - mvDuration);
        
        result.put("note", "Materialized views provide pre-aggregated data for significantly faster queries");
        result.put("testedAt", OffsetDateTime.now());
        
        return result;
    }
    
    @GetMapping("/disk-usage")
    @Operation(summary = "Get disk usage of materialized views")
    public Map<String, Object> getDiskUsage() {
        List<Map<String, Object>> usage = jdbc.queryForList(
            "SELECT " +
            "    schemaname, " +
            "    tablename as view_name, " +
            "    tablename as viewName, " +
            "    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size, " +
            "    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size, " +
            "    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as data_size, " +
            "    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as index_size " +
            "FROM pg_tables " +
            "WHERE tablename LIKE 'mv_%' " +
            "ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC"
        );
        
        return Map.of(
            "views", usage,
            "retrievedAt", OffsetDateTime.now()
        );
    }
}
