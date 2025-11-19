package com.CCM_EV.admin.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class AdminMetricsService {

    private final JdbcTemplate jdbc;
    private final MeterRegistry meterRegistry;

    // Atomic counters for real-time metrics
    private final AtomicInteger totalUsers = new AtomicInteger(0);
    private final AtomicInteger totalTrades = new AtomicInteger(0);
    private final AtomicInteger totalIssuances = new AtomicInteger(0);
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);
    private final AtomicInteger todayActivities = new AtomicInteger(0);
    private final AtomicLong totalTradeVolume = new AtomicLong(0);
    private final AtomicInteger totalCarbonCredits = new AtomicInteger(0);

    // Event counters
    private Counter userRegisteredCounter;
    private Counter userLoginCounter;
    private Counter tradeExecutedCounter;
    private Counter creditIssuedCounter;
    private Counter eventProcessingErrorCounter;
    private Counter paymentCompletedCounter;
    private Counter paymentFailedCounter;

    public AdminMetricsService(JdbcTemplate jdbc, MeterRegistry meterRegistry) {
        this.jdbc = jdbc;
        this.meterRegistry = meterRegistry;
        initializeMetrics();
    }

    private void initializeMetrics() {
        // Register Gauges for database-backed metrics
        Gauge.builder("admin.users.total", totalUsers, AtomicInteger::get)
            .description("Total number of users in the system")
            .tag("type", "dimension")
            .register(meterRegistry);

        Gauge.builder("admin.trades.total", totalTrades, AtomicInteger::get)
            .description("Total number of trades executed")
            .tag("type", "fact")
            .register(meterRegistry);

        Gauge.builder("admin.issuances.total", totalIssuances, AtomicInteger::get)
            .description("Total number of carbon credit issuances")
            .tag("type", "fact")
            .register(meterRegistry);

        Gauge.builder("admin.events.processed.total", totalEventsProcessed, AtomicLong::get)
            .description("Total number of events processed")
            .tag("type", "event")
            .register(meterRegistry);

        Gauge.builder("admin.activities.today", todayActivities, AtomicInteger::get)
            .description("Number of user activities today")
            .tag("type", "activity")
            .register(meterRegistry);

        Gauge.builder("admin.trade.volume.total", totalTradeVolume, AtomicLong::get)
            .description("Total trade volume in VND")
            .tag("currency", "VND")
            .register(meterRegistry);

        Gauge.builder("admin.carbon.credits.total", totalCarbonCredits, AtomicInteger::get)
            .description("Total carbon credits issued (tCO2e)")
            .tag("unit", "tCO2e")
            .register(meterRegistry);

        // Register Counters for event tracking
        userRegisteredCounter = Counter.builder("admin.events.user.registered")
            .description("Number of USER_REGISTERED events processed")
            .tag("event_type", "USER_REGISTERED")
            .register(meterRegistry);

        userLoginCounter = Counter.builder("admin.events.user.login")
            .description("Number of USER_LOGIN events processed")
            .tag("event_type", "USER_LOGIN")
            .register(meterRegistry);

        tradeExecutedCounter = Counter.builder("admin.events.trade.executed")
            .description("Number of TRADE_EXECUTED events processed")
            .tag("event_type", "TRADE_EXECUTED")
            .register(meterRegistry);

        creditIssuedCounter = Counter.builder("admin.events.credit.issued")
            .description("Number of CREDIT_ISSUED events processed")
            .tag("event_type", "CREDIT_ISSUED")
            .register(meterRegistry);

        eventProcessingErrorCounter = Counter.builder("admin.events.processing.errors")
            .description("Number of event processing errors")
            .tag("type", "error")
            .register(meterRegistry);

        paymentCompletedCounter = Counter.builder("admin.events.payment.completed")
            .description("Number of completed payments processed")
            .tag("status", "COMPLETED")
            .register(meterRegistry);

        paymentFailedCounter = Counter.builder("admin.events.payment.failed")
            .description("Number of failed payments processed")
            .tag("status", "FAILED")
            .register(meterRegistry);

        log.info("Admin metrics initialized successfully");
    }

    /**
     * Update metrics from database every 30 seconds
     */
    @Scheduled(fixedDelay = 30000)
    public void updateMetrics() {
        try {
            // Update total users
            Integer users = jdbc.queryForObject("SELECT COUNT(*) FROM dim_users", Integer.class);
            totalUsers.set(users != null ? users : 0);

            // Update total trades
            Integer trades = jdbc.queryForObject("SELECT COUNT(*) FROM fact_trade", Integer.class);
            totalTrades.set(trades != null ? trades : 0);

            // Update total issuances
            Integer issuances = jdbc.queryForObject("SELECT COUNT(*) FROM fact_issuance", Integer.class);
            totalIssuances.set(issuances != null ? issuances : 0);

            // Update total events processed
            Long events = jdbc.queryForObject("SELECT COUNT(*) FROM consumed_events", Long.class);
            totalEventsProcessed.set(events != null ? events : 0);

            // Update today's activities
            Integer activities = jdbc.queryForObject(
                "SELECT COUNT(*) FROM fact_user_activity WHERE occurred_at > CURRENT_DATE",
                Integer.class
            );
            todayActivities.set(activities != null ? activities : 0);

            // Update total trade volume
            Long volume = jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM fact_trade",
                Long.class
            );
            totalTradeVolume.set(volume != null ? volume : 0);

            // Update total carbon credits
            Double credits = jdbc.queryForObject(
                "SELECT COALESCE(SUM(quantity_tco2e), 0) FROM fact_issuance",
                Double.class
            );
            totalCarbonCredits.set(credits != null ? (int)(credits * 1000) : 0); // Convert to milliTCO2e for integer

            log.debug("Metrics updated: users={}, trades={}, issuances={}, events={}", 
                users, trades, issuances, events);
        } catch (Exception e) {
            log.error("Error updating metrics: {}", e.getMessage());
            eventProcessingErrorCounter.increment();
        }
    }

    // Methods to increment event counters (called by consumers)
    public void recordUserRegistered() {
        userRegisteredCounter.increment();
    }

    public void recordUserLogin() {
        userLoginCounter.increment();
    }

    public void recordTradeExecuted() {
        tradeExecutedCounter.increment();
    }

    public void recordCreditIssued() {
        creditIssuedCounter.increment();
    }

    public void recordProcessingError() {
        eventProcessingErrorCounter.increment();
    }

    public void recordPayment(String status) {
        if ("COMPLETED".equalsIgnoreCase(status)) {
            paymentCompletedCounter.increment();
        } else if ("FAILED".equalsIgnoreCase(status)) {
            paymentFailedCounter.increment();
        }
    }
}
