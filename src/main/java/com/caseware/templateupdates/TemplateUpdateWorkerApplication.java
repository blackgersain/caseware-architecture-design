package com.caseware.templateupdates;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.caseware.templateupdates.kafka.KafkaProperties;
import com.caseware.templateupdates.config.WorkerProperties;

@SpringBootApplication
@EnableConfigurationProperties({KafkaProperties.class, WorkerProperties.class})
public class TemplateUpdateWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TemplateUpdateWorkerApplication.class, args);
    }
}