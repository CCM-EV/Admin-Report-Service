package com.CCM_EV.admin.controller;

import com.CCM_EV.admin.entity.SystemLog;
import com.CCM_EV.admin.service.SystemLogService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * System Log Management Controller
 */
@RestController
@RequestMapping("/api/admin/logs")
@RequiredArgsConstructor
@Tag(name = "System Logs", description = "APIs for managing and viewing system logs")
@SecurityRequirement(name = "Bearer Authentication")
@PreAuthorize("hasRole('ADMIN')")
public class SystemLogController {
    
    private final SystemLogService systemLogService;
    
    @GetMapping
    @Operation(summary = "Get system logs", description = "Get paginated system logs with optional filters")
    public ResponseEntity<Page<SystemLog>> getLogs(
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String source,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) String correlationId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime start,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime end,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        Page<SystemLog> logs = systemLogService.getLogs(level, source, category, correlationId, start, end, page, size);
        return ResponseEntity.ok(logs);
    }
    
    @GetMapping("/errors")
    @Operation(summary = "Get error logs", description = "Get paginated error and fatal logs")
    public ResponseEntity<Page<SystemLog>> getErrorLogs(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        Page<SystemLog> logs = systemLogService.getErrorLogs(page, size);
        return ResponseEntity.ok(logs);
    }
    
    @GetMapping("/statistics")
    @Operation(summary = "Get log statistics", description = "Get aggregated log statistics by level")
    public ResponseEntity<Map<String, Object>> getLogStatistics(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime since
    ) {
        Map<String, Object> stats = systemLogService.getLogStatistics(since);
        return ResponseEntity.ok(stats);
    }
    
    @GetMapping("/by-correlation/{correlationId}")
    @Operation(summary = "Get logs by correlation ID", description = "Get all logs related to a correlation ID")
    public ResponseEntity<Page<SystemLog>> getLogsByCorrelationId(
            @PathVariable String correlationId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int size
    ) {
        Page<SystemLog> logs = systemLogService.getLogsByCorrelationId(correlationId, page, size);
        return ResponseEntity.ok(logs);
    }
}
