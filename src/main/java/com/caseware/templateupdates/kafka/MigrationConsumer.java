package com.caseware.templateupdates.kafka;

import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.processing.EngagementWorkProcessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class MigrationConsumer {

    private final EngagementWorkProcessor processor;

    public MigrationConsumer(EngagementWorkProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(
            topics = "${template-update.kafka.migration-topic}",
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
