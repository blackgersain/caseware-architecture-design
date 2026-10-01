package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.domain.WorkType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

class SemaphoreEngagementAdmissionGateTest {

    @Test
    void migrationIsThrottledBeforeTemplateUpdates() {
        var gate = new SemaphoreEngagementAdmissionGate(
                new Semaphore(5),
                new Semaphore(4),
                new Semaphore(1));

        assertThat(gate.tryAcquire(WorkType.MIGRATION)).isTrue();
        assertThat(gate.tryAcquire(WorkType.MIGRATION)).isFalse();

        assertThat(gate.tryAcquire(WorkType.TEMPLATE_UPDATE)).isTrue();
        assertThat(gate.tryAcquire(WorkType.TEMPLATE_UPDATE)).isTrue();
        assertThat(gate.tryAcquire(WorkType.TEMPLATE_UPDATE)).isTrue();
        assertThat(gate.tryAcquire(WorkType.TEMPLATE_UPDATE)).isTrue();
        assertThat(gate.tryAcquire(WorkType.TEMPLATE_UPDATE)).isFalse();
    }
}
