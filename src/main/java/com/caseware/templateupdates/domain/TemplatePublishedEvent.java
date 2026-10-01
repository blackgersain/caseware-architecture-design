package com.caseware.templateupdates.domain;

public record TemplatePublishedEvent(
        String publicationId,
        String templateId,
        String offeredTemplateVersion
) {
}
