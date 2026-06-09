package com.payments.consumer.config;

import com.payments.consumer.model.PaymentInitiatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Explicit consumer configuration.
 * Every setting is annotated to explain the why, not just the what.
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, PaymentInitiatedEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // ── Connectivity ──────────────────────────────────────────────────────
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // ── Identity ──────────────────────────────────────────────────────────
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);


        // ── Deserialisation ───────────────────────────────────────────────────
        // ErrorHandlingDeserializer wraps the real deserializer.
        // If a message cannot be deserialized, instead of crashing the
        // entire listener thread it routes the failure to the error handler,
        // which in turn sends it to the DLT. Without this wrapper, a single
        // poison-pill message would stop all consumption permanently.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JsonDeserializer.class);

        // Target class for JSON deserialization.
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
                PaymentInitiatedEvent.class.getName());

        // Trust these packages so the deserializer accepts the class.
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
                "com.payments.consumer.model,com.payments.events");

        // Use type info from the JSON payload if present; fall back to VALUE_DEFAULT_TYPE.
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);



        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentInitiatedEvent>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentInitiatedEvent> consumerFactory
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentInitiatedEvent>();
        factory.setConsumerFactory(consumerFactory);


        return factory;
    }
}
