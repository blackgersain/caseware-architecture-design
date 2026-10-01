package com.caseware.templateupdates.kafka;

import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.domain.TemplatePublishedEvent;
import com.caseware.templateupdates.domain.WorkType;
import com.caseware.templateupdates.processing.EngagementMetadataRepository;
import com.caseware.templateupdates.processing.WorkDispatchPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TemplatePublishedPlanner {

    private final EngagementMetadataRepository metadataRepository;
    private final WorkDispatchPublisher workDispatchPublisher;

    public TemplatePublishedPlanner(
            EngagementMetadataRepository metadataRepository,
            WorkDispatchPublisher workDispatchPublisher) {
        this.metadataRepository = metadataRepository;
        this.workDispatchPublisher = workDispatchPublisher;
    }

    @KafkaListener(
            topics = "${template-update.kafka.template-published-topic}",
            groupId = "${template-update.kafka.group-id}",
            properties = "spring.json.value.default.type=com.caseware.templateupdates.domain.TemplatePublishedEvent"
    )
    public void consume(TemplatePublishedEvent event) {

        // TODO: This is not safe for large publishes, instead, fan-out should be paged and checkpointed

        metadataRepository.findByTemplate(event.templateId())
                .forEach(metadata -> workDispatchPublisher.publishReview(new EngagementWorkItem(
                        WorkType.TEMPLATE_UPDATE,
                        workId(event, metadata.engagementId()),
                        metadata.region(),
                        metadata.firmId(),
                        metadata.engagementId(),
                        metadata.templateId(),
                        event.offeredTemplateVersion()
                )));
    }

    private static String workId(TemplatePublishedEvent event, String engagementId) {
        return event.publicationId() + ":" + engagementId + ":" + event.offeredTemplateVersion();
    }
}
