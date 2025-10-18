package com.CCM_EV.admin.service;

import com.CCM_EV.admin.repository.ReportingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ReportService {
    private final JdbcTemplate jdbc;
    private final ReportingRepository repo;
    private final MaterializedViewRefreshService mvRefreshService;
    private final PartitionManagementService partitionService;

    public Map<String, Object> overview(OffsetDateTime from, OffsetDateTime to){
        var m = new HashMap<String, Object>();
        
        // Trade metrics
        var tradeMetrics = jdbc.queryForMap("""
                select coalesce(sum(quantity),0) as credits_sold,
                       coalesce(sum(amount),0) as revenue,
                       count(*) as trade_count
                from fact_trade
                where executed_at between ? and ?
                """, from, to);
        m.put("trade", tradeMetrics);
        
        // Issuance metrics
        var issuanceMetrics = jdbc.queryForMap("""
                select coalesce(sum(quantity_tco2e),0) as credits_issued,
                       count(*) as issuance_count
                from fact_issuance
                where issued_at between ? and ?
                """, from, to);
        m.put("issuance", issuanceMetrics);
        
        // Payment metrics
        var paymentMetrics = jdbc.queryForMap("""
                select coalesce(sum(amount),0) as total_payments,
                       count(*) as payment_count
                from fact_payment
                where status = 'COMPLETED' and completed_at between ? and ?
                """, from, to);
        m.put("payment", paymentMetrics);
        
        // User metrics
        var userMetrics = jdbc.queryForMap("""
                select count(distinct user_id) as active_users,
                       count(*) as total_activities
                from fact_user_activity
                where occurred_at between ? and ?
                """, from, to);
        m.put("user", userMetrics);

        return m;
    }

    public List<Map<String, Object>> tradesByDay(OffsetDateTime from, OffsetDateTime to){
        return jdbc.query("""
                select date_trunc('day', executed_at) as day,
                       sum(quantity) as credits_sold, 
                       sum(amount) as revenue,
                       count(*) as trade_count
                from fact_trade
                where executed_at between ? and ?
                group by 1 order by 1 asc
                """, (rs, i) -> Map.of(
                        "day", rs.getTimestamp("day").toInstant().toString(),
                        "credits_sold", rs.getBigDecimal("credits_sold"),
                        "revenue", rs.getBigDecimal("revenue"),
                        "trade_count", rs.getLong("trade_count")
                ), from, to);
    }

    public List<Map<String, Object>> issuancesByDay(OffsetDateTime from, OffsetDateTime to){
        return jdbc.query("""
                select date_trunc('day', issued_at) as day,
                       sum(quantity_tco2e) as credits_issued,
                       count(*) as issuance_count
                from fact_issuance
                where issued_at between ? and ?
                group by 1 order by 1 asc
                """, (rs, i) -> Map.of(
                        "day", rs.getTimestamp("day").toInstant().toString(),
                        "credits_issued", rs.getBigDecimal("credits_issued"),
                        "issuance_count", rs.getLong("issuance_count")
                ), from, to);
    }

    public List<Map<String, Object>> paymentsByDay(OffsetDateTime from, OffsetDateTime to){
        return jdbc.query("""
                select date_trunc('day', completed_at) as day,
                       sum(amount) as total_payments,
                       count(*) as payment_count
                from fact_payment
                where status = 'COMPLETED' and completed_at between ? and ?
                group by 1 order by 1 asc
                """, (rs, i) -> Map.of(
                        "day", rs.getTimestamp("day").toInstant().toString(),
                        "total_payments", rs.getBigDecimal("total_payments"),
                        "payment_count", rs.getLong("payment_count")
                ), from, to);
    }

    public Map<String, Object> userStats(OffsetDateTime from, OffsetDateTime to){
        var m = new HashMap<String, Object>();
        
        // Total users by role
        var usersByRole = jdbc.query("""
                select role, count(*) as count
                from dim_users
                where created_at between ? and ?
                group by role
                """, (rs, i) -> Map.of(
                        "role", rs.getString("role"),
                        "count", rs.getLong("count")
                ), from, to);
        m.put("users_by_role", usersByRole);
        
        // Activity summary
        var activitySummary = jdbc.query("""
                select event_type, count(*) as count
                from fact_user_activity
                where occurred_at between ? and ?
                group by event_type
                """, (rs, i) -> Map.of(
                        "event_type", rs.getString("event_type"),
                        "count", rs.getLong("count")
                ), from, to);
        m.put("activity_summary", activitySummary);
        
        return m;
    }

    public List<Map<String, Object>> userActivities(Long userId, String eventType, int page, int size){
        var sql = new StringBuilder("""
                select ua.user_id, ua.event_type, ua.event_data, ua.occurred_at,
                       u.username, u.email, u.role
                from fact_user_activity ua
                left join dim_users u on ua.user_id = u.user_id
                where 1=1
                """);
        
        if(userId != null) sql.append(" and ua.user_id = ").append(userId);
        if(eventType != null) sql.append(" and ua.event_type = '").append(eventType).append("'");
        
        sql.append(" order by ua.occurred_at desc limit ").append(size)
           .append(" offset ").append(page * size);
        
        return jdbc.query(sql.toString(), (rs, i) -> {
            var map = new HashMap<String, Object>();
            map.put("user_id", rs.getLong("user_id"));
            map.put("username", rs.getString("username"));
            map.put("email", rs.getString("email"));
            map.put("role", rs.getString("role"));
            map.put("event_type", rs.getString("event_type"));
            map.put("event_data", rs.getString("event_data"));
            map.put("occurred_at", rs.getTimestamp("occurred_at").toInstant().toString());
            return map;
        });
    }

    public void refreshMaterializedViews(){
        repo.refreshMaterializedViews();
    }
    
    // Delegate to MaterializedViewRefreshService
    public void refreshView(String viewName, boolean concurrent) {
        mvRefreshService.refreshView(viewName, concurrent);
    }
    
    public List<Map<String, Object>> getMVStatistics() {
        return mvRefreshService.getViewStatistics();
    }
    
    public List<Map<String, Object>> getMVRefreshHistory(String viewName, int limit) {
        return mvRefreshService.getRefreshHistory(viewName, limit);
    }
    
    public List<Map<String, Object>> getAllMVRefreshHistory(int limit) {
        return mvRefreshService.getAllRefreshHistory(limit);
    }
    
    // Delegate to PartitionManagementService
    public void createFuturePartitions() {
        partitionService.createFuturePartitions();
    }
    
    public List<Map<String, Object>> getPartitionInfo(String tableName) {
        return partitionService.getPartitionInfo(tableName);
    }
    
    public void cleanupOldPartitions() {
        partitionService.dropOldPartitions();
    }
}
