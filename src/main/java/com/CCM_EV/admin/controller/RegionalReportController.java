package com.CCM_EV.admin.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Regional Reporting Controller - comprehensive reports by region
 */
@RestController
@RequestMapping("/api/admin/reports")
@RequiredArgsConstructor
@Tag(name = "Regional Reports", description = "Regional analytics and comprehensive reports")
@SecurityRequirement(name = "Bearer Authentication")
@PreAuthorize("hasRole('ADMIN')")
public class RegionalReportController {
    
    private final JdbcTemplate jdbc;
    
    @GetMapping("/regional/co2")
    @Operation(summary = "Get regional CO2 statistics - Optimized with view")
    public Map<String, Object> getRegionalCO2Stats(
            @RequestParam(defaultValue = "30") int days
    ) {
        Map<String, Object> result = new HashMap<>();
        
        // Total by region - USE REGIONAL VIEW (pre-aggregated)
        List<Map<String, Object>> byRegion = jdbc.queryForList("""
            SELECT 
                region,
                SUM(total_tco2e) as total_tco2e,
                SUM(total_distance_km) as total_distance_km,
                SUM(total_energy_kwh) as total_energy_kwh,
                SUM(total_co2_avoided_kg) as total_co2_avoided_kg,
                SUM(issuance_count) as issuance_count
            FROM v_regional_co2_stats
            WHERE date > NOW() - INTERVAL '? days'
            GROUP BY region
            ORDER BY total_tco2e DESC
            """.replace("?", String.valueOf(days)));
        
        result.put("byRegion", byRegion);
        
        // Timeline by region - USE VIEW
        List<Map<String, Object>> timeline = jdbc.queryForList("""
            SELECT 
                date,
                region,
                total_tco2e,
                issuance_count as count,
                total_distance_km,
                total_co2_avoided_kg
            FROM v_regional_co2_stats
            WHERE date > NOW() - INTERVAL '? days'
            ORDER BY date DESC, region
            """.replace("?", String.valueOf(days)));
        
        result.put("timeline", timeline);
        result.put("days", days);
        result.put("note", "Data from v_regional_co2_stats view (last 90 days)");
        
        return result;
    }
    
    @GetMapping("/regional/revenue")
    @Operation(summary = "Get regional revenue statistics - Optimized with view")
    public Map<String, Object> getRegionalRevenueStats(
            @RequestParam(defaultValue = "30") int days
    ) {
        Map<String, Object> result = new HashMap<>();
        
        // Revenue by region - USE REGIONAL VIEW
        List<Map<String, Object>> byRegion = jdbc.queryForList("""
            SELECT 
                region,
                currency,
                SUM(total_amount) as total_revenue,
                SUM(total_quantity) as total_quantity,
                SUM(trade_count) as trade_count,
                AVG(total_amount / NULLIF(trade_count, 0)) as avg_trade_value
            FROM v_regional_trade_stats
            WHERE date > NOW() - INTERVAL '? days'
            GROUP BY region, currency
            ORDER BY total_revenue DESC
            """.replace("?", String.valueOf(days)));
        
        result.put("byRegion", byRegion);
        
        // Timeline by region - USE VIEW
        List<Map<String, Object>> timeline = jdbc.queryForList("""
            SELECT 
                date,
                region,
                currency,
                total_amount as total_revenue,
                trade_count,
                total_quantity
            FROM v_regional_trade_stats
            WHERE date > NOW() - INTERVAL '? days'
            ORDER BY date DESC, region
            """.replace("?", String.valueOf(days)));
        
        result.put("timeline", timeline);
        result.put("days", days);
        result.put("note", "Data from v_regional_trade_stats view (last 90 days)");
        
        return result;
    }
    
    @GetMapping("/regional/transactions")
    @Operation(summary = "Get regional transaction statistics")
    public Map<String, Object> getRegionalTransactionStats(
            @RequestParam(defaultValue = "30") int days
    ) {
        Map<String, Object> result = new HashMap<>();
        
        // Transactions by region
        List<Map<String, Object>> byRegion = jdbc.queryForList("""
            SELECT 
                COALESCE(region, 'Unknown') as region,
                COUNT(*) as transaction_count,
                SUM(CASE WHEN is_auction THEN 1 ELSE 0 END) as auction_count,
                SUM(CASE WHEN is_auction THEN 0 ELSE 1 END) as fixed_count
            FROM fact_trade
            WHERE executed_at > NOW() - INTERVAL '? days'
            GROUP BY COALESCE(region, 'Unknown')
            ORDER BY transaction_count DESC
            """.replace("?", String.valueOf(days)));
        
        result.put("byRegion", byRegion);
        result.put("days", days);
        
        return result;
    }
    
    @GetMapping("/regional/users")
    @Operation(summary = "Get regional user distribution - Optimized with view")
    public Map<String, Object> getRegionalUserStats() {
        Map<String, Object> result = new HashMap<>();
        
        // Users by region and role - USE REGIONAL VIEW
        List<Map<String, Object>> byRegion = jdbc.queryForList("""
            SELECT 
                region,
                role,
                user_count
            FROM v_regional_user_stats
            ORDER BY region, user_count DESC
            """);
        
        result.put("byRegion", byRegion);
        
        // Total summary - USE VIEW
        List<Map<String, Object>> summary = jdbc.queryForList("""
            SELECT 
                region,
                SUM(user_count) as total_users
            FROM v_regional_user_stats
            GROUP BY region
            ORDER BY total_users DESC
            """);
        
        result.put("summary", summary);
        result.put("note", "Data from v_regional_user_stats view");
        
        return result;
    }
    
    @GetMapping("/comprehensive")
    @Operation(summary = "Get comprehensive report - Optimized", 
               description = "Get all metrics (CO2, revenue, transactions, users) using materialized views and optimized queries")
    public Map<String, Object> getComprehensiveReport(
            @RequestParam(defaultValue = "30") int days
    ) {
        Map<String, Object> result = new HashMap<>();
        
        // CO2 metrics - USE MATERIALIZED VIEW for fast aggregation
        Map<String, Object> co2 = jdbc.queryForMap("""
            SELECT 
                COALESCE(SUM(credits_issued), 0) as total_tco2e,
                COUNT(*) as total_issuances,
                COALESCE(SUM(unique_users), 0) as unique_contributors
            FROM mv_issuance_daily
            WHERE day > NOW() - INTERVAL '? days'
            """.replace("?", String.valueOf(days)));
        
        // Additional CO2 details from fact table (partition-aware)
        Map<String, Object> co2Details = jdbc.queryForMap("""
            SELECT 
                COALESCE(SUM(distance_km), 0) as total_distance_km,
                COALESCE(SUM(co2_avoided_kg), 0) as total_co2_avoided_kg,
                COALESCE(SUM(energy_kwh), 0) as total_energy_kwh
            FROM fact_issuance 
            WHERE issued_at > NOW() - INTERVAL '? days'
            """.replace("?", String.valueOf(days)));
        co2.putAll(co2Details);
        result.put("co2_metrics", co2);
        
        // Revenue metrics - USE MATERIALIZED VIEW
        List<Map<String, Object>> revenue = jdbc.queryForList("""
            SELECT 
                'USD' as currency,
                COALESCE(SUM(revenue), 0) as total_revenue,
                COALESCE(AVG(avg_unit_price), 0) as avg_price,
                SUM(trade_count) as trade_count,
                SUM(credits_sold) as total_credits_sold,
                SUM(unique_buyers) as unique_buyers,
                SUM(unique_sellers) as unique_sellers
            FROM mv_trades_daily
            WHERE day > NOW() - INTERVAL '? days'
            """.replace("?", String.valueOf(days)));
        result.put("revenue_metrics", revenue.isEmpty() ? Map.of() : revenue.get(0));
        
        // Transaction metrics - partition-aware with single query
        Map<String, Object> transactions = jdbc.queryForMap("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN listing_id IS NOT NULL THEN 1 ELSE 0 END) as marketplace_trades,
                COUNT(DISTINCT buyer_id) as unique_buyers,
                COUNT(DISTINCT seller_id) as unique_sellers
            FROM fact_trade 
            WHERE executed_at > NOW() - INTERVAL '? days'
            """.replace("?", String.valueOf(days)));
        result.put("transaction_metrics", transactions);
        
        // User metrics - use indexed role column
        Map<String, Object> users = new HashMap<>();
        users.put("total_users", jdbc.queryForObject("SELECT COUNT(*) FROM dim_users", Integer.class));
        users.put("by_role", jdbc.queryForList(
            "SELECT role, COUNT(*) as count FROM dim_users GROUP BY role ORDER BY count DESC"
        ));
        result.put("user_metrics", users);
        
        // Regional breakdown - USE REGIONAL VIEWS (much faster)
        result.put("regional_breakdown", jdbc.queryForList("""
            SELECT 
                COALESCE(t.region, c.region) as region,
                SUM(t.trade_count) as trades,
                SUM(t.total_amount) as revenue,
                SUM(c.issuance_count) as issuances,
                SUM(c.total_tco2e) as tco2e
            FROM v_regional_trade_stats t
            FULL OUTER JOIN v_regional_co2_stats c ON t.region = c.region AND t.date = c.date
            WHERE (t.date > NOW() - INTERVAL '? days' OR c.date > NOW() - INTERVAL '? days')
            GROUP BY COALESCE(t.region, c.region)
            ORDER BY revenue DESC NULLS LAST
            """.replace("?", String.valueOf(days))));
        
        result.put("period_days", days);
        result.put("generated_at", java.time.OffsetDateTime.now());
        result.put("optimization_info", Map.of(
            "uses_materialized_views", true,
            "uses_partition_pruning", true,
            "uses_regional_views", true,
            "query_optimization", "All queries use indexes and pre-aggregated views for optimal performance"
        ));
        
        return result;
    }
    
    @GetMapping("/performance")
    @Operation(summary = "Get system performance metrics")
    public Map<String, Object> getPerformanceMetrics() {
        Map<String, Object> result = new HashMap<>();
        
        // Event processing metrics
        Map<String, Object> eventMetrics = new HashMap<>();
        eventMetrics.put("total_events", jdbc.queryForObject(
            "SELECT COUNT(*) FROM consumed_events", Integer.class
        ));
        eventMetrics.put("events_24h", jdbc.queryForObject(
            "SELECT COUNT(*) FROM consumed_events WHERE received_at > NOW() - INTERVAL '24 hours'", Integer.class
        ));
        eventMetrics.put("events_by_type", jdbc.queryForList(
            "SELECT event_type, COUNT(*) as count FROM consumed_events GROUP BY event_type ORDER BY count DESC"
        ));
        result.put("event_processing", eventMetrics);
        
        // Activity metrics
        Map<String, Object> activityMetrics = new HashMap<>();
        activityMetrics.put("total_activities", jdbc.queryForObject(
            "SELECT COUNT(*) FROM fact_user_activity", Long.class
        ));
        activityMetrics.put("activities_24h", jdbc.queryForObject(
            "SELECT COUNT(*) FROM fact_user_activity WHERE occurred_at > NOW() - INTERVAL '24 hours'", Long.class
        ));
        activityMetrics.put("by_type", jdbc.queryForList(
            "SELECT event_type, COUNT(*) as count FROM fact_user_activity WHERE occurred_at > NOW() - INTERVAL '7 days' GROUP BY event_type ORDER BY count DESC LIMIT 10"
        ));
        result.put("user_activity", activityMetrics);
        
        result.put("generated_at", java.time.OffsetDateTime.now());
        
        return result;
    }
}
