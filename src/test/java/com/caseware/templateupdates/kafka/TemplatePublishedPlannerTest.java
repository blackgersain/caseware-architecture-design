package com.caseware.templateupdates.kafka;

import com.caseware.templateupdates.domain.EngagementMetadataReference;
import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.domain.TemplatePublishedEvent;
import com.caseware.templateupdates.domain.WorkType;
import com.caseware.templateupdates.processing.EngagementMetadataRepository;
import com.caseware.templateupdates.processing.WorkDispatchPublisher;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TemplatePublishedPlannerTest {

    @Test
    void fansOutPublishEventsIntoRegionalEngagementReviewWork() {
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var planner = new TemplatePublishedPlanner(metadataRepository, publisher);
        var event = new TemplatePublishedEvent("publication-1", "template-1", "v7");

        when(metadataRepository.findByTemplate("template-1")).thenReturn(List.of(
                new EngagementMetadataReference("ca-central-1", "firm-1", "engagement-1", "template-1", "v5"),
                new EngagementMetadataReference("ca-central-1", "firm-2", "engagement-2", "template-1", "v6")
        ));

        planner.consume(event);

        verify(publisher).publishReview(new EngagementWorkItem(
                WorkType.TEMPLATE_UPDATE,
                "publication-1:engagement-1:v7",
                "ca-central-1",
                "firm-1",
                "engagement-1",
                "template-1",
                "v7"
        ));
        verify(publisher).publishReview(new EngagementWorkItem(
                WorkType.TEMPLATE_UPDATE,
                "publication-1:engagement-2:v7",
                "ca-central-1",
                "firm-2",
                "engagement-2",
                "template-1",
                "v7"
        ));
    }
}
