package com.caseware.templateupdates.domain;

public record EngagementWorkItem(
        WorkType type,
        String workId,
        String region,
        String firmId,
        String engagementId,
        String templateId,
        String offeredTemplateVersion
) {
}