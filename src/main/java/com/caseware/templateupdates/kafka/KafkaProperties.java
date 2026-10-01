package com.caseware.templateupdates.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "template-update.kafka")
public record KafkaProperties(
        String templatePublishedTopic,
        String engagementUpdateTopic,
        String migrationTopic,
        String groupId
) {
}