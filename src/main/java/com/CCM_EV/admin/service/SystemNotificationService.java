package com.CCM_EV.admin.service;

import com.CCM_EV.admin.entity.SystemNotification;
import com.CCM_EV.admin.repository.SystemNotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SystemNotificationService {
    
    private final SystemNotificationRepository notificationRepository;
    
    @Transactional
    public SystemNotification createNotification(String level, String category, String title, 
                                                 String message, String sourceService, 
                                                 String targetUserId, Integer expiresInHours) {
        SystemNotification notification = SystemNotification.builder()
                .notificationId(UUID.randomUUID().toString())
                .level(level)
                .category(category)
                .title(title)
                .message(message)
                .sourceService(sourceService)
                .targetUserId(targetUserId)
                .readStatus(false)
                .expiresAt(expiresInHours != null ? OffsetDateTime.now().plusHours(expiresInHours) : null)
                .build();
        
        return notificationRepository.save(notification);
    }
    
    @Transactional
    public SystemNotification createBroadcast(String level, String category, String title, 
                                             String message, String sourceService) {
        return createNotification(level, category, title, message, sourceService, null, null);
    }
    
    @Transactional(readOnly = true)
    public Page<SystemNotification> getUserNotifications(String userId, boolean unreadOnly, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        return notificationRepository.findUserNotifications(userId, unreadOnly, pageable);
    }
    
    @Transactional(readOnly = true)
    public Page<SystemNotification> getNotificationsByLevel(String level, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        return notificationRepository.findByLevel(level, pageable);
    }
    
    @Transactional(readOnly = true)
    public Page<SystemNotification> getNotificationsByCategory(String category, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        return notificationRepository.findByCategory(category, pageable);
    }
    
    @Transactional(readOnly = true)
    public long getUnreadCount(String userId) {
        return notificationRepository.countUnreadForUser(userId);
    }
    
    @Transactional
    public boolean markAsRead(Long notificationId) {
        int updated = notificationRepository.markAsRead(notificationId, OffsetDateTime.now());
        return updated > 0;
    }
    
    @Transactional
    public int markAllAsRead(String userId) {
        return notificationRepository.markAllAsReadForUser(userId, OffsetDateTime.now());
    }
    
    @Transactional(readOnly = true)
    public Map<String, Object> getNotificationStatistics(OffsetDateTime since) {
        if (since == null) {
            since = OffsetDateTime.now().minusDays(7);
        }
        
        Map<String, Object> stats = new HashMap<>();
        
        var levelStats = notificationRepository.getStatsByLevel(since);
        Map<String, Long> byLevel = new HashMap<>();
        for (Object[] row : levelStats) {
            byLevel.put((String) row[0], (Long) row[1]);
        }
        
        stats.put("byLevel", byLevel);
        stats.put("since", since);
        stats.put("timestamp", OffsetDateTime.now());
        
        return stats;
    }
    
    @Scheduled(cron = "0 0 * * * *") // Every hour
    @Transactional
    public void cleanupExpiredNotifications() {
        int deleted = notificationRepository.deleteExpired(OffsetDateTime.now());
        if (deleted > 0) {
            log.info("Cleaned up {} expired notifications", deleted);
        }
    }
}
