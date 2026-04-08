package com.example.demo.eand.repo;

import com.example.demo.eand.entity.JobBatchProcessingEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobProcessingRepo extends JpaRepository<JobBatchProcessingEntity, Long> {



    // TODO : can be improved with status filter as well.
    JobBatchProcessingEntity findByJobIdAndBatchIdAndJobType(String jobId, Long batchId, String jobType);

    List<JobBatchProcessingEntity> findByStatusAndWorkerNode(String status, String workerNode);
}