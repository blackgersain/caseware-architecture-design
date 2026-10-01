package com.caseware.templateupdates.domain;

public record EngagementMetadataReference(
        String region,
        String firmId,
        String engagementId,
        String templateId,
        String currentTemplateVersion
) {
}
