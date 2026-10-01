package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.domain.EngagementWorkItem;

public interface WorkDispatchPublisher {

    void publishReview(EngagementWorkItem workItem);

    void publishRetry(EngagementWorkItem workItem, int attempt, String reason);

    void publishDeadLetter(EngagementWorkItem workItem, int attempt, String reason);
}
