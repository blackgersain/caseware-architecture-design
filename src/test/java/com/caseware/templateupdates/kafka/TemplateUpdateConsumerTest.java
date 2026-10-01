package com.caseware.templateupdates.kafka;

import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.domain.WorkType;
import com.caseware.templateupdates.processing.EngagementWorkProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TemplateUpdateConsumerTest {

    @Test
    void acknowledgesOnlyAfterProcessingCompletes() {
        var processor = mock(EngagementWorkProcessor.class);
        var acknowledgment = mock(Acknowledgment.class);
        var future = new CompletableFuture<Void>();
        var consumer = new TemplateUpdateConsumer(processor);
        var workItem = new EngagementWorkItem(
                WorkType.TEMPLATE_UPDATE,
                "work-1",
                "ca-central-1",
                "firm-1",
                "engagement-1",
                "template-1",
                "v7");

        when(processor.processAsync(workItem, 1)).thenReturn(future);

        var returned = consumer.consume(workItem, 1, acknowledgment);

        assertThat(returned.isDone()).isFalse();
        verifyNoInteractions(acknowledgment);

        future.complete(null);
        returned.join();

        verify(acknowledgment).acknowledge();
    }
}
