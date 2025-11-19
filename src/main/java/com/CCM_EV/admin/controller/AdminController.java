package com.CCM_EV.admin.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class AdminController {
    
    private final JdbcTemplate jdbc;

    /**
     * Dashboard Overview - Summary statistics
     */
    @GetMapping("/dashboard/overview")
    public Map<String, Object> getDashboardOverview() {
        Map<String, Object> overview = new HashMap<>();
        
        // Total users
        Integer totalUsers = jdbc.queryForObject("SELECT COUNT(*) FROM dim_users", Integer.class);
        overview.put("totalUsers", totalUsers);
        
        // Users by role
        List<Map<String, Object>> usersByRole = jdbc.queryForList(
            "SELECT role, COUNT(*) as count FROM dim_users GROUP BY role ORDER BY count DESC"
        );
        overview.put("usersByRole", usersByRole);
        
        // Total trades
        Integer totalTrades = jdbc.queryForObject("SELECT COUNT(*) FROM fact_trade", Integer.class);
        overview.put("totalTrades", totalTrades);
        
        // Total trade volume
        Map<String, Object> tradeVolume = jdbc.queryForMap(
            "SELECT SUM(quantity) as total_quantity, SUM(amount) as total_amount, currency FROM fact_trade GROUP BY currency"
        );
        overview.put("tradeVolume", tradeVolume);
        
        // Total carbon credits issued
        Map<String, Object> issuanceStats = jdbc.queryForMap(
            "SELECT COUNT(*) as total_issuances, SUM(quantity_tco2e) as total_tco2e FROM fact_issuance"
        );
        overview.put("carbonCredits", issuanceStats);
        
        // Total events processed
        Integer eventsProcessed = jdbc.queryForObject("SELECT COUNT(*) FROM consumed_events", Integer.class);
        overview.put("eventsProcessed", eventsProcessed);
        
        // Recent activities with details
        List<Map<String, Object>> recentActivities = jdbc.queryForList(
            "SELECT a.event_type, a.user_id, u.username, a.occurred_at " +
            "FROM fact_user_activity a " +
            "LEFT JOIN dim_users u ON a.user_id = u.user_id " +
            "WHERE a.occurred_at > NOW() - INTERVAL '24 hours' " +
            "ORDER BY a.occurred_at DESC LIMIT 10"
        );
        overview.put("recentActivities", recentActivities);
        
        return overview;
    }

    /**
     * User Statistics
     */
    @GetMapping("/users/stats")
    public Map<String, Object> getUserStats() {
        Map<String, Object> stats = new HashMap<>();
        
        // Total users
        stats.put("totalUsers", jdbc.queryForObject("SELECT COUNT(*) FROM dim_users", Integer.class));
        
        // Users by role
        stats.put("byRole", jdbc.queryForList(
            "SELECT role, COUNT(*) as count FROM dim_users GROUP BY role ORDER BY count DESC"
        ));
        
        // Users by region (if available)
        stats.put("byRegion", jdbc.queryForList(
            "SELECT COALESCE(region, 'Unknown') as region, COUNT(*) as count FROM dim_users GROUP BY region ORDER BY count DESC"
        ));
        
        // Recent registrations (last 30 days)
        stats.put("recentRegistrations", jdbc.queryForObject(
            "SELECT COUNT(*) FROM dim_users WHERE created_at > NOW() - INTERVAL '30 days'", 
            Integer.class
        ));
        
        return stats;
    }

    /**
     * User Activity Analytics - Optimized with materialized views and partition pruning
     */
    @GetMapping("/activities")
    public Map<String, Object> getActivityStats(
        @RequestParam(defaultValue = "7") int days
    ) {
        Map<String, Object> stats = new HashMap<>();
        
        // Activity by type - USE MATERIALIZED VIEW (faster aggregation)
        stats.put("byType", jdbc.queryForList(
            "SELECT event_type, SUM(event_count) as count, SUM(unique_users) as unique_users " +
            "FROM mv_user_activity_daily " +
            "WHERE day > NOW() - INTERVAL '" + days + " days' " +
            "GROUP BY event_type ORDER BY count DESC"
        ));
        
        // Activity timeline (daily) - USE MATERIALIZED VIEW
        stats.put("timeline", jdbc.queryForList(
            "SELECT day as date, event_type, event_count as count, unique_users " +
            "FROM mv_user_activity_daily " +
            "WHERE day > NOW() - INTERVAL '" + days + " days' " +
            "ORDER BY day DESC, event_type"
        ));
        
        // Most active users (partition-aware query with composite index)
        stats.put("topUsers", jdbc.queryForList(
            "SELECT u.username, u.role, COUNT(*) as activity_count " +
            "FROM fact_user_activity a " +
            "JOIN dim_users u ON a.user_id = u.user_id " +
            "WHERE a.occurred_at > NOW() - INTERVAL '" + days + " days' " +
            "GROUP BY u.username, u.role " +
            "ORDER BY activity_count DESC " +
            "LIMIT 10"
        ));
        
        return stats;
    }

    /**
     * Trade Statistics - Optimized with materialized views
     */
    @GetMapping("/trades/stats")
    public Map<String, Object> getTradeStats(
        @RequestParam(defaultValue = "30") int days
    ) {
        Map<String, Object> stats = new HashMap<>();
        
        // Total trades (with partition pruning)
        stats.put("totalTrades", jdbc.queryForObject(
            "SELECT COUNT(*) FROM fact_trade WHERE executed_at > NOW() - INTERVAL '" + days + " days'", 
            Integer.class
        ));
        
        // Trade volume by currency (partition-aware)
        stats.put("volumeByCurrency", jdbc.queryForList(
            "SELECT currency, COUNT(*) as trade_count, SUM(quantity) as total_quantity, SUM(amount) as total_amount " +
            "FROM fact_trade WHERE executed_at > NOW() - INTERVAL '90 days' GROUP BY currency"
        ));
        
        // Recent trades (uses idx_fact_trade_executed_at)
        stats.put("recentTrades", jdbc.queryForList(
            "SELECT order_id, buyer_id, seller_id, quantity, unit_price, amount, currency, executed_at " +
            "FROM fact_trade ORDER BY executed_at DESC LIMIT 10"
        ));
        
        // Trade timeline - USE MATERIALIZED VIEW for fast aggregation
        stats.put("timeline", jdbc.queryForList(
            "SELECT day as date, trade_count, revenue as total_amount, avg_unit_price " +
            "FROM mv_trades_daily " +
            "WHERE day > NOW() - INTERVAL '" + days + " days' " +
            "ORDER BY day DESC"
        ));
        
        // Additional stats from MV
        stats.put("buyerSellersStats", jdbc.queryForMap(
            "SELECT SUM(unique_buyers) as total_unique_buyers, SUM(unique_sellers) as total_unique_sellers " +
            "FROM mv_trades_daily WHERE day > NOW() - INTERVAL '" + days + " days'"
        ));
        
        return stats;
    }

    /**
     * Carbon Credit Issuance Statistics - Optimized with materialized views
     */
    @GetMapping("/issuance/stats")
    public Map<String, Object> getIssuanceStats(
        @RequestParam(defaultValue = "30") int days
    ) {
        Map<String, Object> stats = new HashMap<>();
        
        // Total issuances (partition-aware)
        stats.put("totalIssuances", jdbc.queryForObject(
            "SELECT COUNT(*) FROM fact_issuance WHERE issued_at > NOW() - INTERVAL '" + days + " days'", 
            Integer.class
        ));
        
        // Total CO2e issued (partition-aware)
        stats.put("totalTco2e", jdbc.queryForObject(
            "SELECT COALESCE(SUM(quantity_tco2e), 0) FROM fact_issuance WHERE issued_at > NOW() - INTERVAL '" + days + " days'", 
            Double.class
        ));
        
        // Environmental impact (partition-aware for better performance)
        Map<String, Object> impact = new HashMap<>();
        impact.put("totalDistance", jdbc.queryForObject(
            "SELECT COALESCE(SUM(distance_km), 0) FROM fact_issuance WHERE issued_at > NOW() - INTERVAL '90 days'", 
            Double.class
        ));
        impact.put("totalEnergy", jdbc.queryForObject(
            "SELECT COALESCE(SUM(energy_kwh), 0) FROM fact_issuance WHERE issued_at > NOW() - INTERVAL '90 days'", 
            Double.class
        ));
        impact.put("totalCo2Avoided", jdbc.queryForObject(
            "SELECT COALESCE(SUM(co2_avoided_kg), 0) FROM fact_issuance WHERE issued_at > NOW() - INTERVAL '90 days'", 
            Double.class
        ));
        stats.put("environmentalImpact", impact);
        
        // Recent issuances (uses idx_fact_issuance_issued_at)
        stats.put("recentIssuances", jdbc.queryForList(
            "SELECT issuance_id, user_id, quantity_tco2e, distance_km, energy_kwh, co2_avoided_kg, issued_at " +
            "FROM fact_issuance ORDER BY issued_at DESC LIMIT 10"
        ));
        
        // Issuance timeline - USE MATERIALIZED VIEW
        stats.put("timeline", jdbc.queryForList(
            "SELECT day as date, issuance_count as count, credits_issued as total_tco2e, avg_issuance " +
            "FROM mv_issuance_daily " +
            "WHERE day > NOW() - INTERVAL '" + days + " days' " +
            "ORDER BY day DESC"
        ));
        
        // Unique users from MV
        stats.put("uniqueUsers", jdbc.queryForObject(
            "SELECT SUM(unique_users) FROM mv_issuance_daily WHERE day > NOW() - INTERVAL '" + days + " days'",
            Long.class
        ));
        
        return stats;
    }

    /**
     * Event Processing Health
     */
    @GetMapping("/health/events")
    public Map<String, Object> getEventHealth() {
        Map<String, Object> health = new HashMap<>();
        
        // Total events processed
        health.put("totalEvents", jdbc.queryForObject("SELECT COUNT(*) FROM consumed_events", Integer.class));
        
        // Events processed today
        health.put("eventsToday", jdbc.queryForObject(
            "SELECT COUNT(*) FROM consumed_events WHERE received_at > CURRENT_DATE", 
            Integer.class
        ));
        
        // Latest event
        Map<String, Object> latestEvent = jdbc.queryForMap(
            "SELECT event_id, received_at FROM consumed_events ORDER BY received_at DESC LIMIT 1"
        );
        health.put("latestEvent", latestEvent);
        
        // Event processing timeline (hourly for last 24h)
        health.put("processingTimeline", jdbc.queryForList(
            "SELECT DATE_TRUNC('hour', received_at) as hour, COUNT(*) as count " +
            "FROM consumed_events " +
            "WHERE received_at > NOW() - INTERVAL '24 hours' " +
            "GROUP BY DATE_TRUNC('hour', received_at) " +
            "ORDER BY hour DESC"
        ));
        
        return health;
    }

    /**
     * System Metrics for Grafana
     */
    @GetMapping("/metrics/summary")
    public Map<String, Object> getSystemMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Core metrics
        metrics.put("users", jdbc.queryForObject("SELECT COUNT(*) FROM dim_users", Integer.class));
        metrics.put("activities", jdbc.queryForObject("SELECT COUNT(*) FROM fact_user_activity", Integer.class));
        metrics.put("trades", jdbc.queryForObject("SELECT COUNT(*) FROM fact_trade", Integer.class));
        metrics.put("issuances", jdbc.queryForObject("SELECT COUNT(*) FROM fact_issuance", Integer.class));
        metrics.put("eventsProcessed", jdbc.queryForObject("SELECT COUNT(*) FROM consumed_events", Integer.class));
        
        // Calculate success rate (assuming all consumed events are successful)
        Integer totalEvents = (Integer) metrics.get("eventsProcessed");
        metrics.put("eventSuccessRate", totalEvents > 0 ? 100.0 : 0.0);
        
        // Trade volume
        try {
            Map<String, Object> volume = jdbc.queryForMap(
                "SELECT SUM(amount) as total_amount, currency FROM fact_trade GROUP BY currency LIMIT 1"
            );
            metrics.put("tradeVolume", volume);
        } catch (Exception e) {
            metrics.put("tradeVolume", Map.of("total_amount", 0, "currency", "VND"));
        }
        
        // Carbon credits
        Double totalTco2e = jdbc.queryForObject(
            "SELECT COALESCE(SUM(quantity_tco2e), 0) FROM fact_issuance", 
            Double.class
        );
        metrics.put("totalCarbonCredits", totalTco2e);
        
        return metrics;
    }

    /**
     * Time-series data for Grafana charts - Optimized with materialized views
     */
    @GetMapping("/metrics/timeseries")
    public Map<String, Object> getTimeSeriesData(
        @RequestParam(defaultValue = "30") int days
    ) {
        Map<String, Object> timeseries = new HashMap<>();
        
        // User registrations over time (dim table, small size, no MV needed)
        timeseries.put("userRegistrations", jdbc.queryForList(
            "SELECT DATE(created_at) as date, COUNT(*) as count " +
            "FROM dim_users " +
            "WHERE created_at > NOW() - INTERVAL '" + days + " days' " +
            "GROUP BY DATE(created_at) " +
            "ORDER BY date"
        ));
        
        // Trade volume over time - USE MATERIALIZED VIEW
        timeseries.put("tradeVolume", jdbc.queryForList(
            "SELECT day as date, trade_count as trades, revenue as volume, " +
            "       credits_sold, avg_unit_price, unique_buyers, unique_sellers " +
            "FROM mv_trades_daily " +
            "WHERE day > NOW() - INTERVAL '" + days + " days' " +
            "ORDER BY day"
        ));
        
        // Carbon credits issued over time - USE MATERIALIZED VIEW
        timeseries.put("carbonIssuance", jdbc.queryForList(
            "SELECT day as date, issuance_count as count, credits_issued as total_tco2e, " +
            "       avg_issuance, unique_users " +
            "FROM mv_issuance_daily " +
            "WHERE day > NOW() - INTERVAL '" + days + " days' " +
            "ORDER BY day"
        ));
        
        // Event processing rate (uses idx_consumed_events_received_at)
        timeseries.put("eventProcessing", jdbc.queryForList(
            "SELECT DATE(received_at) as date, COUNT(*) as count " +
            "FROM consumed_events " +
            "WHERE received_at > NOW() - INTERVAL '" + days + " days' " +
            "GROUP BY DATE(received_at) " +
            "ORDER BY date"
        ));
        
        return timeseries;
    }

    /**
     * Data quality and integrity report
     */
    @GetMapping("/health/data-quality")
    public Map<String, Object> getDataQuality() {
        Map<String, Object> quality = new HashMap<>();
        
        // Check for orphaned records
        Integer orphanedActivities = jdbc.queryForObject(
            "SELECT COUNT(*) FROM fact_user_activity a " +
            "WHERE NOT EXISTS (SELECT 1 FROM dim_users u WHERE u.user_id = a.user_id)", 
            Integer.class
        );
        quality.put("orphanedActivities", orphanedActivities);
        
        // Check for duplicate events
        Integer duplicateEvents = jdbc.queryForObject(
            "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM consumed_events", 
            Integer.class
        );
        quality.put("duplicateEvents", duplicateEvents);
        
        // Data completeness
        Integer usersWithActivities = jdbc.queryForObject(
            "SELECT COUNT(DISTINCT user_id) FROM fact_user_activity", 
            Integer.class
        );
        Integer totalUsers = jdbc.queryForObject("SELECT COUNT(*) FROM dim_users", Integer.class);
        quality.put("usersWithActivities", usersWithActivities);
        quality.put("totalUsers", totalUsers);
        quality.put("activityCoverage", totalUsers > 0 ? (usersWithActivities * 100.0 / totalUsers) : 0);
        
        // Overall health status
        boolean isHealthy = orphanedActivities == 0 && duplicateEvents == 0;
        quality.put("overallHealth", isHealthy ? "HEALTHY" : "NEEDS_ATTENTION");
        
        return quality;
    }
}
