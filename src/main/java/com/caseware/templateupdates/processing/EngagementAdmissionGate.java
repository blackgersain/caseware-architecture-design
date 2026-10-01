package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.domain.WorkType;

public interface EngagementAdmissionGate {

    boolean tryAcquire(WorkType workType);

    void release(WorkType workType);
}
