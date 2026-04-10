package com.example.demo.eand.repo;

import com.example.demo.eand.entity.BatchJobProcessEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;



@Repository
public interface BatchJobProcessEntityRepo extends JpaRepository<BatchJobProcessEntity, Long> {

    List<BatchJobProcessEntity> findByJobId(String jobId);

    List<BatchJobProcessEntity> findByJobIdAndBatchId(String jobId, Long batchId);

    List<BatchJobProcessEntity> findByJobIdAndBatchIdAndStatus(String jobId, Long batchId, String status);

    List<BatchJobProcessEntity> findByJobIdAndStatus(String jobId, String status);

    BatchJobProcessEntity findByJobIdAndBatchIdAndJobType(String jobId, Long batchId, String jobType);

    List<BatchJobProcessEntity> findByStatusAndWorkerNode(String status, String workerNode);

    long countByJobIdAndBatchIdAndJobTypeAndStatusAndActiveIndIn(String jobId, Long batchId, String jobType, String status, Collection<Boolean> activeInds);

}