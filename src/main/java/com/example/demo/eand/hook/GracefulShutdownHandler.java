package com.example.demo.eand.hook;
import com.example.demo.eand.entity.JobBatchProcessingEntity;
import com.example.demo.eand.enums.BatchStatusEnum;
import com.example.demo.eand.repo.JobProcessingRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
@Slf4j
@Component
@RequiredArgsConstructor
public class GracefulShutdownHandler {

    private final JobProcessingRepo jobProcessingRepo;

    @EventListener(ContextClosedEvent.class)
    public void handleContextClosed(ContextClosedEvent event) {
        log.warn("SHUTDOWN INITIATED - Marking all Batch in-progress jobs as FAILED");
        markInProgressJobsAsFailed();
    }

    private void markInProgressJobsAsFailed() {
        try {
            // TODO : testing need to be done for this hook
            // Find all jobs with PROCESSING status
            List<JobBatchProcessingEntity> processingJobs = jobProcessingRepo.findByStatusAndWorkerNode(BatchStatusEnum.PROCESSING.name() ,workerNodeName() );

            if (processingJobs.isEmpty()) {
                log.info("No processing jobs found");
                return;
            }

            processingJobs.forEach(job -> {
                job.setStatus(BatchStatusEnum.FAILED.name());
                job.setCompletedAt(LocalDateTime.now());
                jobProcessingRepo.save(job);
                log.warn("Marked job as FAILED: jobId={}, batchId={}, jobType={}", job.getJobId(), job.getBatchId(), job.getJobType());
            });

            log.warn("✓ Successfully marked {} job(s) as FAILED", processingJobs.size());

        } catch (Exception ex) {
            log.error("Error marking in-progress jobs as FAILED", ex);
        }
    }

    private String workerNodeName() {
        return "Worker_localhost_node";
    }


}