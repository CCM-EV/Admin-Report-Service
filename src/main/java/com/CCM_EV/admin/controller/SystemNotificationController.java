package com.CCM_EV.admin.controller;

import com.CCM_EV.admin.entity.SystemNotification;
import com.CCM_EV.admin.service.SystemNotificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * System Notification Management Controller
 */
@RestController
@RequestMapping("/api/admin/notifications")
@RequiredArgsConstructor
@Tag(name = "System Notifications", description = "APIs for managing system notifications")
@SecurityRequirement(name = "Bearer Authentication")
public class SystemNotificationController {
    
    private final SystemNotificationService notificationService;
    
    @PostMapping
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(summary = "Create notification", description = "Create a new system notification")
    public ResponseEntity<SystemNotification> createNotification(
            @RequestParam String level,
            @RequestParam String category,
            @RequestParam String title,
            @RequestParam String message,
            @RequestParam String sourceService,
            @RequestParam(required = false) String targetUserId,
            @RequestParam(required = false) Integer expiresInHours
    ) {
        SystemNotification notification = notificationService.createNotification(
                level, category, title, message, sourceService, targetUserId, expiresInHours
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(notification);
    }
    
    @PostMapping("/broadcast")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(summary = "Create broadcast notification", description = "Create a broadcast notification for all users")
    public ResponseEntity<SystemNotification> createBroadcast(
            @RequestParam String level,
            @RequestParam String category,
            @RequestParam String title,
            @RequestParam String message,
            @RequestParam String sourceService
    ) {
        SystemNotification notification = notificationService.createBroadcast(
                level, category, title, message, sourceService
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(notification);
    }
    
    @GetMapping("/user/{userId}")
    @Operation(summary = "Get user notifications", description = "Get notifications for a specific user")
    public ResponseEntity<Page<SystemNotification>> getUserNotifications(
            @PathVariable String userId,
            @RequestParam(defaultValue = "false") boolean unreadOnly,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        Page<SystemNotification> notifications = notificationService.getUserNotifications(userId, unreadOnly, page, size);
        return ResponseEntity.ok(notifications);
    }
    
    @GetMapping("/level/{level}")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(summary = "Get notifications by level", description = "Get notifications filtered by level")
    public ResponseEntity<Page<SystemNotification>> getNotificationsByLevel(
            @PathVariable String level,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        Page<SystemNotification> notifications = notificationService.getNotificationsByLevel(level, page, size);
        return ResponseEntity.ok(notifications);
    }
    
    @GetMapping("/category/{category}")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(summary = "Get notifications by category", description = "Get notifications filtered by category")
    public ResponseEntity<Page<SystemNotification>> getNotificationsByCategory(
            @PathVariable String category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        Page<SystemNotification> notifications = notificationService.getNotificationsByCategory(category, page, size);
        return ResponseEntity.ok(notifications);
    }
    
    @GetMapping("/user/{userId}/unread-count")
    @Operation(summary = "Get unread count", description = "Get count of unread notifications for a user")
    public ResponseEntity<Map<String, Long>> getUnreadCount(@PathVariable String userId) {
        long count = notificationService.getUnreadCount(userId);
        return ResponseEntity.ok(Map.of("unreadCount", count));
    }
    
    @PatchMapping("/{notificationId}/read")
    @Operation(summary = "Mark as read", description = "Mark a notification as read")
    public ResponseEntity<Void> markAsRead(@PathVariable Long notificationId) {
        boolean success = notificationService.markAsRead(notificationId);
        return success ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }
    
    @PatchMapping("/user/{userId}/mark-all-read")
    @Operation(summary = "Mark all as read", description = "Mark all notifications as read for a user")
    public ResponseEntity<Map<String, Integer>> markAllAsRead(@PathVariable String userId) {
        int count = notificationService.markAllAsRead(userId);
        return ResponseEntity.ok(Map.of("markedCount", count));
    }
    
    @GetMapping("/statistics")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(summary = "Get notification statistics", description = "Get aggregated notification statistics")
    public ResponseEntity<Map<String, Object>> getStatistics(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime since
    ) {
        Map<String, Object> stats = notificationService.getNotificationStatistics(since);
        return ResponseEntity.ok(stats);
    }
}
