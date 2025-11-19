package com.CCM_EV.admin.repository;

import com.CCM_EV.admin.entity.SystemNotification;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface SystemNotificationRepository extends JpaRepository<SystemNotification, Long> {
    
    Optional<SystemNotification> findByNotificationId(String notificationId);
    
    Page<SystemNotification> findByTargetUserIdAndReadStatus(String targetUserId, Boolean readStatus, Pageable pageable);
    
    Page<SystemNotification> findByTargetUserIdIsNullAndReadStatus(Boolean readStatus, Pageable pageable);
    
    Page<SystemNotification> findByLevel(String level, Pageable pageable);
    
    Page<SystemNotification> findByCategory(String category, Pageable pageable);
    
    @Query("SELECT sn FROM SystemNotification sn WHERE " +
           "(sn.targetUserId = :userId OR sn.targetUserId IS NULL) AND " +
           "(:unreadOnly = false OR sn.readStatus = false) " +
           "ORDER BY sn.createdAt DESC")
    Page<SystemNotification> findUserNotifications(
        @Param("userId") String userId,
        @Param("unreadOnly") boolean unreadOnly,
        Pageable pageable
    );
    
    @Query("SELECT COUNT(sn) FROM SystemNotification sn WHERE " +
           "(sn.targetUserId = :userId OR sn.targetUserId IS NULL) AND " +
           "sn.readStatus = false")
    long countUnreadForUser(@Param("userId") String userId);
    
    @Modifying
    @Query("UPDATE SystemNotification sn SET sn.readStatus = true, sn.readAt = :readAt " +
           "WHERE sn.id = :id AND sn.readStatus = false")
    int markAsRead(@Param("id") Long id, @Param("readAt") OffsetDateTime readAt);
    
    @Modifying
    @Query("UPDATE SystemNotification sn SET sn.readStatus = true, sn.readAt = :readAt " +
           "WHERE (sn.targetUserId = :userId OR sn.targetUserId IS NULL) AND sn.readStatus = false")
    int markAllAsReadForUser(@Param("userId") String userId, @Param("readAt") OffsetDateTime readAt);
    
    @Modifying
    @Query("DELETE FROM SystemNotification sn WHERE sn.expiresAt IS NOT NULL AND sn.expiresAt < :now")
    int deleteExpired(@Param("now") OffsetDateTime now);
    
    @Query("SELECT sn.level, COUNT(sn) FROM SystemNotification sn " +
           "WHERE sn.createdAt > :since GROUP BY sn.level")
    List<Object[]> getStatsByLevel(@Param("since") OffsetDateTime since);
}
