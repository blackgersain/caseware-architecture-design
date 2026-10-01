package com.caseware.templateupdates.engagement;

import com.caseware.templateupdates.domain.EngagementMetadataSnapshot;
import com.caseware.templateupdates.domain.EngagementWorkItem;

public interface EngagementManagement {

    EngagementMetadataSnapshot loadMetadata(EngagementWorkItem workItem);
}