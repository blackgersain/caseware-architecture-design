package com.caseware.templateupdates.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "template-update.worker")
public record WorkerProperties(
        int maxConcurrentEngagements,
        int reservedForUsers,
        int templateUpdateConcurrency,
        int migrationConcurrency,
        int maxAttempts,
        String retryTopic,
        String deadLetterTopic
) {
}
