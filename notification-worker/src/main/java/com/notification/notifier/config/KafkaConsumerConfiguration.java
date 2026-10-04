package com.notification.notifier.config;

import com.notification.events.InvoiceDueEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConsumerConfiguration {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InvoiceDueEvent> kafkaListenerContainerFactory(
        ConsumerFactory<String, InvoiceDueEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, InvoiceDueEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.setBatchListener(false);

        log.debug("KafkaConsumerConfiguration: Configured listener container with concurrency=3, ack=RECORD");

        return factory;
    }
}
