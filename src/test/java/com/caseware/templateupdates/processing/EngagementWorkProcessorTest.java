package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.config.WorkerProperties;
import com.caseware.templateupdates.domain.EngagementMetadataSnapshot;
import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.domain.MetadataWriteResult;
import com.caseware.templateupdates.domain.RetryableEngagementProcessingException;
import com.caseware.templateupdates.domain.WorkType;
import com.caseware.templateupdates.engagement.EngagementManagement;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class EngagementWorkProcessorTest {

    @Test
    void processesWorkWhenCapacityIsAvailable() {
        var engagementManagement = mock(EngagementManagement.class);
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var admissionGate = mock(EngagementAdmissionGate.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var executor = directExecutor();

        when(admissionGate.tryAcquire(WorkType.TEMPLATE_UPDATE)).thenReturn(true);
        when(engagementManagement.loadMetadata(templateUpdate())).thenReturn(templateUpdateSnapshot());
        when(metadataRepository.upsertIfNewer(templateUpdateSnapshot())).thenReturn(MetadataWriteResult.UPDATED);

        var processor = new EngagementWorkProcessor(
                engagementManagement,
                metadataRepository,
                admissionGate,
                publisher,
                executor,
                workerProperties());

        processor.processAsync(templateUpdate(), 1).join();

        verify(engagementManagement).loadMetadata(templateUpdate());
        verify(metadataRepository).upsertIfNewer(templateUpdateSnapshot());
        verify(admissionGate).tryAcquire(WorkType.TEMPLATE_UPDATE);
        verify(admissionGate).release(WorkType.TEMPLATE_UPDATE);
        verifyNoInteractions(publisher);
        executor.shutdownNow();
    }

    @Test
    void republishesWhenCapacityIsExhausted() {
        var engagementManagement = mock(EngagementManagement.class);
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var admissionGate = mock(EngagementAdmissionGate.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var executor = directExecutor();

        when(admissionGate.tryAcquire(WorkType.TEMPLATE_UPDATE)).thenReturn(false);

        var processor = new EngagementWorkProcessor(
                engagementManagement,
                metadataRepository,
                admissionGate,
                publisher,
                executor,
                workerProperties());

        processor.processAsync(templateUpdate(), 1).join();

        verify(admissionGate).tryAcquire(WorkType.TEMPLATE_UPDATE);
        verifyNoInteractions(engagementManagement);
        verifyNoInteractions(metadataRepository);
        verify(publisher).publishRetry(templateUpdate(), 2, "worker capacity exhausted");
        verifyNoMoreInteractions(admissionGate);
        executor.shutdownNow();
    }

    @Test
    void republishesRetryableFailureWhenAttemptsRemain() {
        var engagementManagement = mock(EngagementManagement.class);
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var admissionGate = mock(EngagementAdmissionGate.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var executor = directExecutor();

        when(admissionGate.tryAcquire(WorkType.TEMPLATE_UPDATE)).thenReturn(true);
        doThrow(new RetryableEngagementProcessingException("downstream unavailable"))
                .when(engagementManagement).loadMetadata(templateUpdate());

        var processor = new EngagementWorkProcessor(
                engagementManagement,
                metadataRepository,
                admissionGate,
                publisher,
                executor,
                workerProperties());

        processor.processAsync(templateUpdate(), 1).join();

        verify(admissionGate).tryAcquire(WorkType.TEMPLATE_UPDATE);
        verify(publisher).publishRetry(templateUpdate(), 2, "downstream unavailable");
        verify(admissionGate).release(WorkType.TEMPLATE_UPDATE);
        executor.shutdownNow();
    }

    @Test
    void deadLettersWhenRetriesAreExhausted() {
        var engagementManagement = mock(EngagementManagement.class);
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var admissionGate = mock(EngagementAdmissionGate.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var executor = directExecutor();

        when(metadataRepository.exists("engagement-1")).thenReturn(false);
        when(admissionGate.tryAcquire(WorkType.MIGRATION)).thenReturn(true);
        doThrow(new RetryableEngagementProcessingException("downstream unavailable"))
                .when(engagementManagement).loadMetadata(migration());

        var processor = new EngagementWorkProcessor(
                engagementManagement,
                metadataRepository,
                admissionGate,
                publisher,
                executor,
                workerProperties());

        processor.processAsync(migration(), 3).join();

        verify(admissionGate).tryAcquire(WorkType.MIGRATION);
        verify(publisher).publishDeadLetter(migration(), 3, "downstream unavailable");
        verify(admissionGate).release(WorkType.MIGRATION);
        executor.shutdownNow();
    }

    @Test
    void deadLettersPermanentFailureImmediately() {
        var engagementManagement = mock(EngagementManagement.class);
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var admissionGate = mock(EngagementAdmissionGate.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var executor = directExecutor();

        when(admissionGate.tryAcquire(WorkType.TEMPLATE_UPDATE)).thenReturn(true);
        doThrow(new IllegalStateException("bad payload"))
                .when(engagementManagement).loadMetadata(templateUpdate());

        var processor = new EngagementWorkProcessor(
                engagementManagement,
                metadataRepository,
                admissionGate,
                publisher,
                executor,
                workerProperties());

        processor.processAsync(templateUpdate(), 1).join();

        verify(publisher).publishDeadLetter(templateUpdate(), 1, "bad payload");
        verify(admissionGate).release(WorkType.TEMPLATE_UPDATE);
        executor.shutdownNow();
    }

    @Test
    void skipsMigrationWhenMetadataAlreadyExists() {
        var engagementManagement = mock(EngagementManagement.class);
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var admissionGate = mock(EngagementAdmissionGate.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var executor = directExecutor();

        when(metadataRepository.exists("engagement-1")).thenReturn(true);

        var processor = new EngagementWorkProcessor(
                engagementManagement,
                metadataRepository,
                admissionGate,
                publisher,
                executor,
                workerProperties());

        processor.processAsync(migration(), 1).join();

        verify(metadataRepository).exists("engagement-1");
        verifyNoInteractions(engagementManagement);
        verifyNoInteractions(admissionGate);
        verifyNoInteractions(publisher);
        executor.shutdownNow();
    }

    @Test
    void ignoresStaleSnapshotsThroughConditionalUpsert() {
        var engagementManagement = mock(EngagementManagement.class);
        var metadataRepository = mock(EngagementMetadataRepository.class);
        var admissionGate = mock(EngagementAdmissionGate.class);
        var publisher = mock(WorkDispatchPublisher.class);
        var executor = directExecutor();

        when(admissionGate.tryAcquire(WorkType.TEMPLATE_UPDATE)).thenReturn(true);
        when(engagementManagement.loadMetadata(templateUpdate())).thenReturn(templateUpdateSnapshot());
        when(metadataRepository.upsertIfNewer(templateUpdateSnapshot())).thenReturn(MetadataWriteResult.NOOP_STALE);

        var processor = new EngagementWorkProcessor(
                engagementManagement,
                metadataRepository,
                admissionGate,
                publisher,
                executor,
                workerProperties());

        processor.processAsync(templateUpdate(), 1).join();

        verify(metadataRepository).upsertIfNewer(templateUpdateSnapshot());
        verifyNoInteractions(publisher);
        executor.shutdownNow();
    }

    private static WorkerProperties workerProperties() {
        return new WorkerProperties(20, 5, 10, 5, 3, "engagementReview.retry", "engagementReview.dlq");
    }

    private static EngagementWorkItem templateUpdate() {
        return new EngagementWorkItem(
                WorkType.TEMPLATE_UPDATE,
                "work-1",
                "ca-central-1",
                "firm-1",
                "engagement-1",
                "template-1",
                "v7"
        );
    }

    private static EngagementWorkItem migration() {
        return new EngagementWorkItem(
                WorkType.MIGRATION,
                "migration-1",
                "ca-central-1",
                "firm-1",
                "engagement-1",
                "template-1",
                null
        );
    }

    private static EngagementMetadataSnapshot templateUpdateSnapshot() {
        return new EngagementMetadataSnapshot(
                "ca-central-1",
                "firm-1",
                "engagement-1",
                "template-1",
                "v5",
                "v7"
        );
    }

    private static ExecutorService directExecutor() {
        return Executors.newSingleThreadExecutor();
    }
}
