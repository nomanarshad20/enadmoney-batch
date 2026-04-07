package com.example.demo.eand.service;

import com.example.demo.eand.client.EandClient;
import com.example.demo.eand.dto.BatchConfigDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;

import com.example.demo.eand.entity.JobBatchProcessingEntity;
import com.example.demo.eand.enums.BatchStatusEnum;
import com.example.demo.eand.pc.ConsumerClient;
import com.example.demo.eand.repo.JobProcessingRepo;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@AllArgsConstructor
public class JobProcessingTemplateServiceImpl implements JobProcessingTemplateService {

    private final JobProcessingRepo jobProcessingRepo;
    @PersistenceContext
    private EntityManager entityManager;



    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //-------------------------- Pulgin Exposed methods initiateJobBatching --------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------


    /**
     * Initiate the job batching
     * @param batchConfigDTO
     */
    @Override
    public void initiateJobBatching(BatchConfigDTO batchConfigDTO) {

        // STEP 1 : Preprocessing and prepare the batch list DTO
        List<JobBatchProcessingDto> batchList = preprocessingJobBatchTemplate(batchConfigDTO);

        //STEP 2 : Save the batch list to DB
        saveJobBatchTemplate(batchList);

        //STEP 3 : Submit the batch list to the queue
        submitJobBatchTemplate(batchList);
    }


    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //-------------------------- Private methods below -----------------------------------------------
    //--------------------------------Inner Working---------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------


    /**
     * Preprocessing the batch list DTO
     * @param batchConfigDTO
     * @return List<JobBatchProcessingDto>
     */
    private List<JobBatchProcessingDto> preprocessingJobBatchTemplate(BatchConfigDTO batchConfigDTO) {
        List<JobBatchProcessingDto> preparingBatchList = new ArrayList<>();
        String jobId = UUID.randomUUID().toString();
        Long batchId = 1L;
        long startId = 0L;
        long endId = 0L;

        while (true) {
            JobBatchProcessingDto dto = prepareDefaultDTO(batchConfigDTO, jobId, batchId, startId);
            List<Long> userIdsList = fetchUserIds(batchConfigDTO.getJpaSqlCommand(), startId, batchConfigDTO.getPaginationSize());
            if (null == userIdsList || userIdsList.isEmpty()) {
                log.info("No more records found. Stopping processing.");
                break;
            }
            log.info("Fetched batch: {}", userIdsList);
            endId = userIdsList.get(userIdsList.size() - 1);
            dto.setEndId(endId);
            startId = endId;
            batchId++;
            preparingBatchList.add(dto);
        }

        return preparingBatchList;
    }


    /**
     * Fetch the user ids from the database
     * @param sql
     * @param startId
     * @param pageSize
     * @return List<Long>
     */
    @Transactional(readOnly = true)
    private List<Long> fetchUserIds(String sql, Long startId, int pageSize) {

        @SuppressWarnings("unchecked")
        List<Long> rawResults = entityManager.createNativeQuery(sql)
                .setParameter("startId", startId)
                .setParameter("pageSize", pageSize)
                .getResultList();
        return rawResults;
    }


    private void saveJobBatchTemplate(List<JobBatchProcessingDto> jobBatchProcessingDto) {
        List<JobBatchProcessingEntity> savedBatchEntityList = new ArrayList<>();
        jobBatchProcessingDto.forEach(batch -> {
            log.info("Job Batch Template : {}", batch);
            savedBatchEntityList.add(toEntity(batch));

        });
        jobProcessingRepo.saveAll(savedBatchEntityList);
    }

    /**
     * Submit the batch list to the queue
     * @param preparingBatchList
     */
    private void submitJobBatchTemplate(List<JobBatchProcessingDto> preparingBatchList) {
        // SUBMIT JOB BATCH TEMPLATE TO QUEUE
        // NOTE : Eand will be responsible for submitting the job to the queue
        preparingBatchList.forEach(batch -> log.info("Job Batch Template : {}", batch));

        preparingBatchList.stream().forEach(batch -> {
            log.info("Job Batch Template : {}", batch);
            EandClient.callPostAPI(batch);
        });

    }


    private JobBatchProcessingEntity toEntity(JobBatchProcessingDto dto) {
        return JobBatchProcessingEntity.builder()
                .jobId(dto.getJobId())
                .batchId(dto.getBatchId())
                .startId(dto.getStartId())
                .endId(dto.getEndId())
                .totalRecords(dto.getTotalRecords())
                .retryCount(dto.getRetryCount() != null ? dto.getRetryCount() : 1) // Default : 1
                .status(dto.getStatus() != null ? dto.getStatus() : BatchStatusEnum.PENDING.name())
                .workerNode(dto.getWorkerNode())
                .paginationSize(dto.getPaginationSize())
                .executorPoolSize(dto.getExecutorPoolSize())
                .batchChunkSize(dto.getBatchChunkSize())
                .jobType(dto.getJobType().name())
                .build();
    }

    private JobBatchProcessingDto prepareDefaultDTO(BatchConfigDTO batchConfigDTO, String jobId, Long batchId, long startId) {
        return JobBatchProcessingDto.builder()
                .jobId(jobId)
                .batchId(batchId)
                .startId(startId)
                .jobType(batchConfigDTO.getJobType())
                .status(BatchStatusEnum.PENDING.name())
                .retryCount(batchConfigDTO.getRetryCount())
                .workerNode(workerNodeName())
                .batchChunkSize(batchConfigDTO.getBatchChunkSize())
                .paginationSize(batchConfigDTO.getPaginationSize())
                .executorPoolSize(batchConfigDTO.getExecutorPoolSize())
                .build();
    }


    private String workerNodeName() {
        return "Worker_localhost_node";
    }


    private JobBatchProcessingDto toDTO(JobBatchProcessingEntity entity) {
        return JobBatchProcessingDto.builder()
                .id(entity.getId())
                .jobId(entity.getJobId())
                .batchId(entity.getBatchId())
                .startId(entity.getStartId())
                .endId(entity.getEndId())
                .totalRecords(entity.getTotalRecords())
                .processedRecords(entity.getProcessedRecords())
                .failedRecords(entity.getFailedRecords())
                .status(entity.getStatus())
                .retryCount(entity.getRetryCount())
                .workerNode(entity.getWorkerNode())
                .createdAt(entity.getCreatedAt())
                .startedAt(entity.getStartedAt())
                .completedAt(entity.getCompletedAt())
                .paginationSize(entity.getPaginationSize())
                .executorPoolSize(entity.getExecutorPoolSize())
                .build();
    }

}
