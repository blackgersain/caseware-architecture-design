package com.caseware.templateupdates.processing;

import com.caseware.templateupdates.domain.EngagementMetadataReference;
import com.caseware.templateupdates.domain.EngagementMetadataSnapshot;
import com.caseware.templateupdates.domain.MetadataWriteResult;

import java.util.List;

public interface EngagementMetadataRepository {

    boolean exists(String engagementId);

    List<EngagementMetadataReference> findByTemplate(String templateId);

    MetadataWriteResult upsertIfNewer(EngagementMetadataSnapshot snapshot);
}
