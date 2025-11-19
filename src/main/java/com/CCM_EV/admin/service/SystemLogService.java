package com.CCM_EV.admin.service;

import com.CCM_EV.admin.entity.SystemLog;
import com.CCM_EV.admin.repository.SystemLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SystemLogService {
    
    private final SystemLogRepository systemLogRepository;
    
    @Transactional
    public SystemLog createLog(String level, String source, String category, 
                               String message, String details, String correlationId, String userId) {
        SystemLog systemLog = SystemLog.builder()
                .logLevel(level)
                .sourceService(source)
                .category(category)
                .message(message)
                .details(details)
                .correlationId(correlationId)
                .userId(userId)
                .build();
        
        return systemLogRepository.save(systemLog);
    }
    
    @Transactional(readOnly = true)
    public Page<SystemLog> getLogs(String level, String source, String category, 
                                   String correlationId, OffsetDateTime start, 
                                   OffsetDateTime end, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "logTimestamp"));
        
        if (start == null) {
            start = OffsetDateTime.now().minusDays(7); // Default last 7 days
        }
        if (end == null) {
            end = OffsetDateTime.now();
        }
        
        return systemLogRepository.findByFilters(level, source, category, correlationId, start, end, pageable);
    }
    
    @Transactional(readOnly = true)
    public Page<SystemLog> getErrorLogs(int page, int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "logTimestamp"));
        return systemLogRepository.findByLogLevelIn(List.of("ERROR", "FATAL"), pageable);
    }
    
    @Transactional(readOnly = true)
    public Map<String, Object> getLogStatistics(OffsetDateTime since) {
        if (since == null) {
            since = OffsetDateTime.now().minusDays(1); // Default last 24 hours
        }
        
        Map<String, Object> stats = new HashMap<>();
        stats.put("debug", systemLogRepository.countByLevelSince("DEBUG", since));
        stats.put("info", systemLogRepository.countByLevelSince("INFO", since));
        stats.put("warn", systemLogRepository.countByLevelSince("WARN", since));
        stats.put("error", systemLogRepository.countByLevelSince("ERROR", since));
        stats.put("fatal", systemLogRepository.countByLevelSince("FATAL", since));
        stats.put("since", since);
        stats.put("timestamp", OffsetDateTime.now());
        
        return stats;
    }
    
    @Transactional(readOnly = true)
    public Page<SystemLog> getLogsByCorrelationId(String correlationId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.ASC, "logTimestamp"));
        OffsetDateTime start = OffsetDateTime.now().minusDays(30);
        OffsetDateTime end = OffsetDateTime.now();
        return systemLogRepository.findByFilters(null, null, null, correlationId, start, end, pageable);
    }
}
