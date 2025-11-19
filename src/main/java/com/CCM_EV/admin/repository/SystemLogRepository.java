package com.CCM_EV.admin.repository;

import com.CCM_EV.admin.entity.SystemLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

@Repository
public interface SystemLogRepository extends JpaRepository<SystemLog, Long> {
    
    Page<SystemLog> findByLogLevel(String logLevel, Pageable pageable);
    
    Page<SystemLog> findBySourceService(String sourceService, Pageable pageable);
    
    Page<SystemLog> findByCategory(String category, Pageable pageable);
    
    Page<SystemLog> findByLogTimestampBetween(OffsetDateTime start, OffsetDateTime end, Pageable pageable);
    
    @Query("SELECT sl FROM SystemLog sl WHERE sl.logLevel IN :levels ORDER BY sl.logTimestamp DESC")
    Page<SystemLog> findByLogLevelIn(@Param("levels") List<String> levels, Pageable pageable);
    
    @Query("SELECT sl FROM SystemLog sl WHERE " +
           "(:level IS NULL OR sl.logLevel = :level) AND " +
           "(:source IS NULL OR sl.sourceService = :source) AND " +
           "(:category IS NULL OR sl.category = :category) AND " +
           "(:correlationId IS NULL OR sl.correlationId = :correlationId) AND " +
           "sl.logTimestamp BETWEEN :start AND :end")
    Page<SystemLog> findByFilters(
        @Param("level") String level,
        @Param("source") String source,
        @Param("category") String category,
        @Param("correlationId") String correlationId,
        @Param("start") OffsetDateTime start,
        @Param("end") OffsetDateTime end,
        Pageable pageable
    );
    
    @Query("SELECT COUNT(sl) FROM SystemLog sl WHERE sl.logLevel = :level AND sl.logTimestamp > :since")
    long countByLevelSince(@Param("level") String level, @Param("since") OffsetDateTime since);
}
