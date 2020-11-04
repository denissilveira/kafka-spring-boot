package com.learnkafka.config;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Log4j2
@EnableTransactionManagement
public class KafkaProducerConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapAddress;

    @Value("${spring.kafka.producer.properties.transactional.id}")
    private String transactionId;

    @Value("${spring.kafka.producer.properties.acks}")
    private String acks;

    @Value("${spring.kafka.producer.properties.retries}")
    private String retries;

    @Bean
    public KafkaProducer<Integer, String> libraryEventsProducer() {
        return new KafkaProducer<>(producerConfigs());
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager() {
        KafkaTransactionManager ktm = new KafkaTransactionManager<>(producerFactory());
        ktm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
        return ktm;
    }

    ProducerFactory producerFactory() {
        var factory = new DefaultKafkaProducerFactory<>(producerConfigs());
        factory.setTransactionIdPrefix("library-events-");
        return factory;
    }

    Map<String, Object> producerConfigs() {
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        //Kafka will use this transaction id as part of its algorithm to deduplicate any message this producer sends, ensuring idempotency.
        return props;
    }

}
