package com.caseware.templateupdates.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Configuration
public class WorkerConfiguration {

    @Bean
    public Semaphore sharedEngagementSemaphore(WorkerProperties workerProperties) {
        return new Semaphore(workerCapacity(workerProperties));
    }

    @Bean
    public Semaphore templateUpdateSemaphore(WorkerProperties workerProperties) {
        return new Semaphore(workerProperties.templateUpdateConcurrency());
    }

    @Bean
    public Semaphore migrationSemaphore(WorkerProperties workerProperties) {
        return new Semaphore(workerProperties.migrationConcurrency());
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService engagementExecutor(WorkerProperties workerProperties) {
        int workerCapacity = workerCapacity(workerProperties);

        return new ThreadPoolExecutor(
                0,
                workerCapacity,
                60,
                TimeUnit.SECONDS,
                new SynchronousQueue<>()
        );
    }

    private static int workerCapacity(WorkerProperties workerProperties) {
        int workerCapacity = workerProperties.maxConcurrentEngagements() - workerProperties.reservedForUsers();
        if (workerCapacity <= 0) {
            throw new IllegalArgumentException("reservedForUsers must be smaller than maxConcurrentEngagements");
        }

        int topicCapacity = workerProperties.templateUpdateConcurrency() + workerProperties.migrationConcurrency();
        if (topicCapacity > workerCapacity) {
            throw new IllegalArgumentException("topic capacity must not exceed available worker capacity");
        }

        return workerCapacity;
    }
}