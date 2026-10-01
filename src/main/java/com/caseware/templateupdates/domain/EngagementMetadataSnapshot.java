package com.caseware.templateupdates.domain;

public record EngagementMetadataSnapshot(
        String region,
        String firmId,
        String engagementId,
        String templateId,
        String currentTemplateVersion,
        String pendingTemplateVersion
) {
}
