package com.CCM_EV.admin.repository;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ReportingRepository {
    private final JdbcTemplate jdbc;
    public ReportingRepository(JdbcTemplate jdbc){ this.jdbc = jdbc; }

    public boolean tryMarkConsumed(String eventId){
        try {
            jdbc.update("insert into consumed_events(event_id) values (?)", eventId);
            return true;
        } catch (DuplicateKeyException e){ return false; }
    }

    // Trade operations
    public void upsertTrade(long orderId, long buyerId, long sellerId, Long listingId,
                            double qty, String unit, double unitPrice, double amount,
                            String currency, String executedAtIso){
        jdbc.update("""
      insert into fact_trade(order_id,buyer_id,seller_id,listing_id,quantity,unit,unit_price,amount,currency,executed_at)
      values (?,?,?,?,?,?,?,?,?,?::timestamptz)
      on conflict (order_id) do update set
        buyer_id=excluded.buyer_id, seller_id=excluded.seller_id, listing_id=excluded.listing_id,
        quantity=excluded.quantity, unit=excluded.unit, unit_price=excluded.unit_price,
        amount=excluded.amount, currency=excluded.currency, executed_at=excluded.executed_at
      """, String.valueOf(orderId), buyerId, sellerId, listingId != null ? String.valueOf(listingId) : null,
              qty, unit, unitPrice, amount, currency, executedAtIso);
    }

    // Payment operations
    public void upsertPayment(String paymentId, String orderId, double amount, String currency, 
                              String status, String paymentMethod, String completedAtIso){
        jdbc.update("""
      insert into fact_payment(payment_id, order_id, amount, currency, status, payment_method, completed_at)
      values (?,?,?,?,?,?,?::timestamptz)
      on conflict (payment_id) do update set
        order_id=excluded.order_id, amount=excluded.amount, currency=excluded.currency,
        status=excluded.status, payment_method=excluded.payment_method, completed_at=excluded.completed_at
      """, paymentId, orderId, amount, currency, status, paymentMethod, completedAtIso);
    }

    // Issuance operations
    public void upsertIssuance(String issuanceId, long userId, String requestId, double quantityTco2e,
                               Double distanceKm, Double energyKwh, Double co2AvoidedKg, String issuedAtIso){
        jdbc.update("""
      insert into fact_issuance(issuance_id, user_id, request_id, quantity_tco2e, distance_km, energy_kwh, co2_avoided_kg, issued_at)
      values (?,?,?,?,?,?,?,?::timestamptz)
      on conflict (issuance_id) do update set
        user_id=excluded.user_id, request_id=excluded.request_id, quantity_tco2e=excluded.quantity_tco2e,
        distance_km=excluded.distance_km, energy_kwh=excluded.energy_kwh, 
        co2_avoided_kg=excluded.co2_avoided_kg, issued_at=excluded.issued_at
      """, issuanceId, userId, requestId, quantityTco2e, distanceKm, energyKwh, co2AvoidedKg, issuedAtIso);
    }

    // User operations
    public void upsertUser(long userId, String username, String email, String role, 
                          String org, String region, String createdAtIso){
        jdbc.update("""
      insert into dim_users(user_id, username, email, role, org, region, created_at)
      values (?,?,?,?,?,?,?::timestamptz)
      on conflict (user_id) do update set
        username=excluded.username, email=excluded.email, role=excluded.role,
        org=excluded.org, region=excluded.region, created_at=excluded.created_at
      """, userId, username, email, role, org, region, createdAtIso);
    }

    // User activity tracking
    public void insertUserActivity(long userId, String eventType, String eventData, String occurredAtIso){
        jdbc.update("""
      insert into fact_user_activity(user_id, event_type, event_data, occurred_at)
      values (?,?,?::jsonb,?::timestamptz)
      """, userId, eventType, eventData, occurredAtIso);
    }

    // Refresh materialized views
    public void refreshMaterializedViews(){
        jdbc.execute("refresh materialized view concurrently mv_trades_daily");
        jdbc.execute("refresh materialized view concurrently mv_issuance_daily");
        jdbc.execute("refresh materialized view concurrently mv_payments_daily");
    }
}
