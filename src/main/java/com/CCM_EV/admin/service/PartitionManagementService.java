package com.CCM_EV.admin.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;

/**
 * Service for automatic partition management
 * - Creates future partitions automatically
 * - Drops old partitions based on retention policy
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PartitionManagementService {

    private final JdbcTemplate jdbcTemplate;
    
    private static final int PARTITIONS_TO_CREATE_AHEAD = 3;
    
    /**
     * Scheduled job to create future partitions
     * Runs daily at 2 AM
     */
    @Scheduled(cron = "0 0 2 * * ?")
    @Transactional
    public void createFuturePartitions() {
        log.info("Starting automatic partition creation job");
        
        try {
            List<Map<String, Object>> tables = jdbcTemplate.queryForList(
                "SELECT table_name, last_partition_date, retention_months FROM partition_metadata"
            );
            
            for (Map<String, Object> table : tables) {
                String tableName = (String) table.get("table_name");
                LocalDate lastPartitionDate = ((java.sql.Date) table.get("last_partition_date")).toLocalDate();
                
                createPartitionsForTable(tableName, lastPartitionDate);
            }
            
            log.info("Automatic partition creation job completed successfully");
        } catch (Exception e) {
            log.error("Error in automatic partition creation job", e);
        }
    }
    
    /**
     * Create partitions for a specific table
     */
    public void createPartitionsForTable(String tableName, LocalDate lastPartitionDate) {
        LocalDate now = LocalDate.now();
        LocalDate targetDate = now.plusMonths(PARTITIONS_TO_CREATE_AHEAD);
        
        LocalDate currentPartitionStart = lastPartitionDate;
        
        while (currentPartitionStart.isBefore(targetDate) || currentPartitionStart.isEqual(targetDate)) {
            LocalDate nextPartitionStart = currentPartitionStart.plusMonths(1);
            
            String partitionName = createPartition(tableName, currentPartitionStart, nextPartitionStart);
            
            if (partitionName != null) {
                log.info("Created partition: {} for table: {}", partitionName, tableName);
                
                // Update metadata
                jdbcTemplate.update(
                    "UPDATE partition_metadata SET last_partition_date = ?, updated_at = now() WHERE table_name = ?",
                    java.sql.Date.valueOf(nextPartitionStart),
                    tableName
                );
            }
            
            currentPartitionStart = nextPartitionStart;
        }
    }
    
    /**
     * Create a single partition using the helper function
     */
    private String createPartition(String tableName, LocalDate startDate, LocalDate endDate) {
        try {
            String result = jdbcTemplate.queryForObject(
                "SELECT create_next_partition(?, ?, ?)",
                String.class,
                tableName,
                java.sql.Date.valueOf(startDate),
                java.sql.Date.valueOf(endDate)
            );
            return result;
        } catch (Exception e) {
            log.error("Error creating partition for table: {} from {} to {}", 
                     tableName, startDate, endDate, e);
            return null;
        }
    }
    
    /**
     * Drop old partitions based on retention policy
     * Runs monthly on the 1st day at 3 AM
     */
    @Scheduled(cron = "0 0 3 1 * ?")
    @Transactional
    public void dropOldPartitions() {
        log.info("Starting old partition cleanup job");
        
        try {
            List<Map<String, Object>> tables = jdbcTemplate.queryForList(
                "SELECT table_name, retention_months FROM partition_metadata"
            );
            
            for (Map<String, Object> table : tables) {
                String tableName = (String) table.get("table_name");
                Integer retentionMonths = (Integer) table.get("retention_months");
                
                dropPartitionsForTable(tableName, retentionMonths);
            }
            
            log.info("Old partition cleanup job completed successfully");
        } catch (Exception e) {
            log.error("Error in old partition cleanup job", e);
        }
    }
    
    /**
     * Drop old partitions for a specific table
     */
    private void dropPartitionsForTable(String tableName, int retentionMonths) {
        LocalDate cutoffDate = LocalDate.now().minusMonths(retentionMonths);
        YearMonth cutoffMonth = YearMonth.from(cutoffDate);
        
        try {
            // Get all partitions for this table
            String sql = """
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename LIKE ? || '_%'
                AND tablename ~ '^.*_[0-9]{4}_[0-9]{2}$'
                """;
            
            List<Map<String, Object>> partitions = jdbcTemplate.queryForList(sql, tableName);
            
            for (Map<String, Object> partition : partitions) {
                String partitionName = (String) partition.get("tablename");

                String[] parts = partitionName.split("_");
                if (parts.length >= 2) {
                    try {
                        int year = Integer.parseInt(parts[parts.length - 2]);
                        int month = Integer.parseInt(parts[parts.length - 1]);
                        
                        YearMonth partitionMonth = YearMonth.of(year, month);
                        
                        if (partitionMonth.isBefore(cutoffMonth)) {
                            dropPartition(partitionName);
                            log.info("Dropped old partition: {}", partitionName);
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Could not parse date from partition name: {}", partitionName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error dropping old partitions for table: {}", tableName, e);
        }
    }
    
    /**
     * Drop a single partition
     */
    private void dropPartition(String partitionName) {
        try {
            jdbcTemplate.execute(String.format("DROP TABLE IF EXISTS %s", partitionName));
        } catch (Exception e) {
            log.error("Error dropping partition: {}", partitionName, e);
        }
    }
    
    /**
     * Manual partition creation for specific month
     */
    public void createPartitionManually(String tableName, YearMonth yearMonth) {
        LocalDate startDate = yearMonth.atDay(1);
        LocalDate endDate = yearMonth.plusMonths(1).atDay(1);
        
        String partitionName = createPartition(tableName, startDate, endDate);
        
        if (partitionName != null) {
            log.info("Manually created partition: {}", partitionName);
        } else {
            throw new RuntimeException("Failed to create partition for " + tableName + " at " + yearMonth);
        }
    }
    
    /**
     * Get partition information
     */
    public List<Map<String, Object>> getPartitionInfo(String tableName) {
        String sql = """
            SELECT 
                schemaname,
                tablename as partition_name,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
            FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename LIKE ? || '_%'
            ORDER BY tablename
            """;
        
        return jdbcTemplate.queryForList(sql, tableName);
    }
}
