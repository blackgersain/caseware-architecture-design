package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.domain.RetryableEngagementProcessingException;
import com.caseware.templateupdates.domain.WorkType;
import com.caseware.templateupdates.engagement.EngagementManagement;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@Component
public class EngagementWorkProcessor {

    private final EngagementManagement engagementManagement;
    private final EngagementMetadataRepository metadataRepository;
    private final EngagementAdmissionGate admissionGate;
    private final WorkDispatchPublisher workDispatchPublisher;
    private final ExecutorService engagementExecutor;
    private final int maxAttempts;

    public EngagementWorkProcessor(
            EngagementManagement engagementManagement,
            EngagementMetadataRepository metadataRepository,
            EngagementAdmissionGate admissionGate,
            WorkDispatchPublisher workDispatchPublisher,
            ExecutorService engagementExecutor,
            com.caseware.templateupdates.config.WorkerProperties workerProperties) {
        this.engagementManagement = engagementManagement;
        this.metadataRepository = metadataRepository;
        this.admissionGate = admissionGate;
        this.workDispatchPublisher = workDispatchPublisher;
        this.engagementExecutor = engagementExecutor;
        this.maxAttempts = workerProperties.maxAttempts();
    }

    public CompletableFuture<Void> processAsync(EngagementWorkItem workItem, int attempt) {
        if (workItem.type() == WorkType.MIGRATION && metadataRepository.exists(workItem.engagementId())) {
            return CompletableFuture.completedFuture(null);
        }

        if (!admissionGate.tryAcquire(workItem.type())) {
            republish(workItem, attempt, "worker capacity exhausted");
            return CompletableFuture.completedFuture(null);
        }

        try {
            return CompletableFuture.runAsync(() -> processWithPermit(workItem, attempt), engagementExecutor);
        } catch (RejectedExecutionException ex) {
            admissionGate.release(workItem.type());
            republish(workItem, attempt, "executor rejected task");
            return CompletableFuture.completedFuture(null);
        }
    }

    private void processWithPermit(EngagementWorkItem workItem, int attempt) {
        try {
            var snapshot = engagementManagement.loadMetadata(workItem);
            metadataRepository.upsertIfNewer(snapshot);
        } catch (RetryableEngagementProcessingException ex) {
            republish(workItem, attempt, ex.getMessage());
        } catch (RuntimeException ex) {
            workDispatchPublisher.publishDeadLetter(workItem, attempt, ex.getMessage());
        } finally {
            admissionGate.release(workItem.type());
        }
    }

    private void republish(EngagementWorkItem workItem, int attempt, String reason) {
        if (attempt >= maxAttempts) {
            workDispatchPublisher.publishDeadLetter(workItem, attempt, reason);
            return;
        }

        workDispatchPublisher.publishRetry(workItem, attempt + 1, reason);
    }
}