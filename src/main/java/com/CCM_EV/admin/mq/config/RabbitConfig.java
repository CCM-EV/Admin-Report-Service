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

    // Queues for marketplace service
    @Bean Queue qTrade()    { return QueueBuilder.durable("admin.trade-executed.v1").withArgument("x-dead-letter-exchange", EX_DLX).build(); }
    @Bean Queue qPayment()  { return QueueBuilder.durable("admin.payment-completed.v1").withArgument("x-dead-letter-exchange", EX_DLX).build(); }
    
    // Queues for carbon module
    @Bean Queue qIssuance() { return QueueBuilder.durable("admin.credit-issued.v1").withArgument("x-dead-letter-exchange", EX_DLX).build(); }
    
    // Queues for auth service
    @Bean Queue qUserRegistered() { return QueueBuilder.durable("admin.user-registered.v1").withArgument("x-dead-letter-exchange", EX_DLX).build(); }
    @Bean Queue qUserLoggedIn()   { return QueueBuilder.durable("admin.user-loggedin.v1").withArgument("x-dead-letter-exchange", EX_DLX).build(); }

    // Bindings for marketplace service
    @Bean Binding bTrade()    { return BindingBuilder.bind(qTrade()).to(events()).with("marketplace.trade.executed.v1"); }
    @Bean Binding bPayment()  { return BindingBuilder.bind(qPayment()).to(events()).with("marketplace.payment.completed.v1"); }
    
    // Bindings for carbon module
    @Bean Binding bIssuance() { return BindingBuilder.bind(qIssuance()).to(events()).with("carbon.credit.issued.v1"); }
    
    // Bindings for auth service
    @Bean Binding bUserRegistered() { return BindingBuilder.bind(qUserRegistered()).to(events()).with("auth.user.registered.v1"); }
    @Bean Binding bUserLoggedIn()   { return BindingBuilder.bind(qUserLoggedIn()).to(events()).with("auth.user.loggedin.v1"); }

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
