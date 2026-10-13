package com.notification.invoice.config;

import com.notification.events.InvoiceDueTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic invoiceDueTopic(
            @Value("${application.kafka.topic.partitions}") int partitions,
            @Value("${application.kafka.topic.replication-factor}") int replicationFactor) {
        return TopicBuilder.name(InvoiceDueTopics.MAIN)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}