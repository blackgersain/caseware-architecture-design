package com.caseware.templateupdates.kafka;

import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.processing.EngagementWorkProcessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;

import java.util.concurrent.CompletableFuture;

@Component
public class TemplateUpdateConsumer {

    private final EngagementWorkProcessor processor;

    public TemplateUpdateConsumer(
            EngagementWorkProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(
            topics = "${template-update.kafka.engagement-update-topic}",
            groupId = "${template-update.kafka.group-id}"
    )
    public CompletableFuture<Void> consume(
            EngagementWorkItem workItem,
            @Header(name = "x-attempt", defaultValue = "1") int attempt,
            Acknowledgment acknowledgment) {

        return processor.processAsync(workItem, attempt)
                .thenRun(acknowledgment::acknowledge);
    }
}