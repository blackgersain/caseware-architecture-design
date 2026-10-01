package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.config.WorkerProperties;
import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class KafkaWorkDispatchPublisher implements WorkDispatchPublisher {

    private final KafkaTemplate<String, EngagementWorkItem> kafkaTemplate;
    private final KafkaProperties kafkaProperties;
    private final WorkerProperties workerProperties;

    public KafkaWorkDispatchPublisher(
            KafkaTemplate<String, EngagementWorkItem> kafkaTemplate,
            KafkaProperties kafkaProperties,
            WorkerProperties workerProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
        this.workerProperties = workerProperties;
    }

    @Override
    public void publishReview(EngagementWorkItem workItem) {
        kafkaTemplate.send(MessageBuilder.withPayload(workItem)
                .setHeader(KafkaHeaders.TOPIC, kafkaProperties.engagementUpdateTopic())
                .setHeader(KafkaHeaders.KEY, workItem.workId())
                .build()).join();
    }

    @Override
    public void publishRetry(EngagementWorkItem workItem, int attempt, String reason) {
        kafkaTemplate.send(MessageBuilder.withPayload(workItem)
                .setHeader(KafkaHeaders.TOPIC, workerProperties.retryTopic())
                .setHeader(KafkaHeaders.KEY, workItem.workId())
                .setHeader("x-attempt", attempt)
                .setHeader("x-reason", reason)
                .build()).join();
    }

    @Override
    public void publishDeadLetter(EngagementWorkItem workItem, int attempt, String reason) {
        kafkaTemplate.send(MessageBuilder.withPayload(workItem)
                .setHeader(KafkaHeaders.TOPIC, workerProperties.deadLetterTopic())
                .setHeader(KafkaHeaders.KEY, workItem.workId())
                .setHeader("x-attempt", attempt)
                .setHeader("x-reason", reason)
                .build()).join();
    }
}
