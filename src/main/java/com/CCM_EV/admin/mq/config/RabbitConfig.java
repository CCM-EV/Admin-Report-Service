package com.CCM_EV.admin.mq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {
    public static final String EX_EVENTS = "co2.events";
    public static final String EX_DLX    = "co2.events.dlx";

    @Bean TopicExchange events() { return new TopicExchange(EX_EVENTS, true, false); }
    @Bean TopicExchange dlx()    { return new TopicExchange(EX_DLX, true, false); }

    // Queues for different services
    @Bean Queue qUserEvents()     { return QueueBuilder.durable("admin.user.events").withArgument("x-dead-letter-exchange", EX_DLX).build(); }
    @Bean Queue qTradeEvents()    { return QueueBuilder.durable("admin.trade.events").withArgument("x-dead-letter-exchange", EX_DLX).build(); }
    @Bean Queue qIssuanceEvents() { return QueueBuilder.durable("admin.issuance.events").withArgument("x-dead-letter-exchange", EX_DLX).build(); }
    @Bean Queue qPaymentEvents()  { return QueueBuilder.durable("admin.payment.events").withArgument("x-dead-letter-exchange", EX_DLX).build(); }

    // Bindings - User events from Auth service
    @Bean Binding bUserRegistered() { return BindingBuilder.bind(qUserEvents()).to(events()).with("auth.user.registered"); }
    @Bean Binding bUserLoggedIn()   { return BindingBuilder.bind(qUserEvents()).to(events()).with("auth.user.loggedin"); }
    @Bean Binding bUserUpdated()    { return BindingBuilder.bind(qUserEvents()).to(events()).with("auth.user.updated"); }
    @Bean Binding bUserDeleted()    { return BindingBuilder.bind(qUserEvents()).to(events()).with("auth.user.deleted"); }
    
    // Bindings - Trade events from Marketplace service
    @Bean Binding bTradeCreated()    { return BindingBuilder.bind(qTradeEvents()).to(events()).with("marketplace.trade.created"); }
    @Bean Binding bTradeUpdated()    { return BindingBuilder.bind(qTradeEvents()).to(events()).with("marketplace.trade.updated"); }
    @Bean Binding bTradeCompleted()  { return BindingBuilder.bind(qTradeEvents()).to(events()).with("marketplace.trade.completed"); }
    @Bean Binding bTradeCancelled()  { return BindingBuilder.bind(qTradeEvents()).to(events()).with("marketplace.trade.cancelled"); }
    
    // Bindings - Issuance events from Carbon service
    @Bean Binding bIssuanceApproved() { return BindingBuilder.bind(qIssuanceEvents()).to(events()).with("carbon.issuance.approved"); }
    @Bean Binding bCreditRequested()  { return BindingBuilder.bind(qIssuanceEvents()).to(events()).with("carbon.credit.requested"); }
    @Bean Binding bCreditIssued()     { return BindingBuilder.bind(qIssuanceEvents()).to(events()).with("carbon.credit.issued"); }
    @Bean Binding bCreditRejected()   { return BindingBuilder.bind(qIssuanceEvents()).to(events()).with("carbon.credit.rejected"); }
    
    // Bindings - Payment events from Payment service
    // Matches Payment publisher routing keys: payment.payment.created|success|failed
    @Bean Binding bPaymentCreated()  { return BindingBuilder.bind(qPaymentEvents()).to(events()).with("payment.payment.created"); }
    @Bean Binding bPaymentSuccess()  { return BindingBuilder.bind(qPaymentEvents()).to(events()).with("payment.payment.success"); }
    @Bean Binding bPaymentFailed()   { return BindingBuilder.bind(qPaymentEvents()).to(events()).with("payment.payment.failed"); }

    @Bean public MessageConverter jackson2MessageConverter(ObjectMapper om){ return new Jackson2JsonMessageConverter(om); }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory cf, MessageConverter mc) {
        var f = new SimpleRabbitListenerContainerFactory();
        f.setConnectionFactory(cf);
        f.setMessageConverter(mc);
        f.setConcurrentConsumers(2);
        f.setMaxConcurrentConsumers(8);
        f.setPrefetchCount(100);
        f.setDefaultRequeueRejected(false);
        return f;
    }
}
