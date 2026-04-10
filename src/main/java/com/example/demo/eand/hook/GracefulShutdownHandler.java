package com.example.demo.eand.hook;
import com.example.demo.eand.entity.BatchJobProcessEntity;
import com.example.demo.eand.enums.BatchJobStatusEnum;
import com.example.demo.eand.repo.BatchJobProcessEntityRepo;
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
    private final BatchJobProcessEntityRepo batchJobProcessEntityRepo;


    @EventListener(ContextClosedEvent.class)
    public void handleContextClosed(ContextClosedEvent event) {
        log.warn("SHUTDOWN HOOK : INITIATED - Marking all Batch in-progress jobs as FAILED");
        markInProgressJobsAsFailed();
    }

    private void markInProgressJobsAsFailed() {
        try {
            // Find all jobs with PROCESSING status
            List<BatchJobProcessEntity> processingJobs = batchJobProcessEntityRepo.findByStatusAndWorkerNode(BatchJobStatusEnum.PROCESSING.name() ,workerNodeName() );
            if (processingJobs.isEmpty()) {
                log.info("SHUTDOWN HOOK : No processing jobs found to shutdown and mark as FAILED.");
                return;
            }

            processingJobs.forEach(job -> {
                job.setStatus(BatchJobStatusEnum.FAILED.name());
                job.setCompletedAt(LocalDateTime.now());
                batchJobProcessEntityRepo.save(job);
                log.warn("SHUTDOWN HOOK : Marked job as FAILED: jobId={}, batchId={}, jobType={}", job.getJobId(), job.getBatchId(), job.getJobType());
            });
        } catch (Exception ex) {
            log.error("SHUTDOWN HOOK : Error marking in-progress jobs as FAILED", ex);
        }
    }

    private String workerNodeName() {
        return "Worker_localhost_node";
    }

}