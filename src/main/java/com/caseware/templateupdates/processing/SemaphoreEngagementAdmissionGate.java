package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.config.WorkerProperties;
import com.caseware.templateupdates.domain.WorkType;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;

@Component
public class SemaphoreEngagementAdmissionGate implements EngagementAdmissionGate {

    private final Semaphore shared;
    private final Semaphore templateUpdates;
    private final Semaphore migrations;

    public SemaphoreEngagementAdmissionGate(
            @Qualifier("sharedEngagementSemaphore") Semaphore shared,
            @Qualifier("templateUpdateSemaphore") Semaphore templateUpdates,
            @Qualifier("migrationSemaphore") Semaphore migrations) {
        this.shared = shared;
        this.templateUpdates = templateUpdates;
        this.migrations = migrations;
    }

    @Override
    public boolean tryAcquire(WorkType workType) {
        var topicSemaphore = semaphore(workType);
        if (!topicSemaphore.tryAcquire()) {
            return false;
        }

        if (shared.tryAcquire()) {
            return true;
        }

        topicSemaphore.release();
        return false;
    }

    @Override
    public void release(WorkType workType) {
        shared.release();
        semaphore(workType).release();
    }

    private Semaphore semaphore(WorkType workType) {
        return switch (workType) {
            case TEMPLATE_UPDATE -> templateUpdates;
            case MIGRATION -> migrations;
        };
    }
}
