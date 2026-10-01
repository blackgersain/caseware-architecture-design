package com.caseware.templateupdates;

import org.junit.jupiter.api.Test;
import com.caseware.templateupdates.engagement.EngagementManagement;
import com.caseware.templateupdates.domain.EngagementWorkItem;
import com.caseware.templateupdates.processing.EngagementMetadataRepository;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import static org.mockito.Mockito.mock;

@SpringBootTest
@Import(TemplateUpdateWorkerApplicationTests.TestBeans.class)
class TemplateUpdateWorkerApplicationTests {

    @Test
    void contextLoads() {
    }

    @TestConfiguration
    static class TestBeans {

        @Bean
        EngagementManagement engagementManagement() {
            return mock(EngagementManagement.class);
        }

        @Bean
        EngagementMetadataRepository engagementMetadataRepository() {
            return mock(EngagementMetadataRepository.class);
        }

        @Bean
        KafkaTemplate<String, EngagementWorkItem> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }
    }
}
