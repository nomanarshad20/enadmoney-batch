package com.example.demo.eand.publisher;

import com.example.demo.eand.client.EandClient;
import com.example.demo.eand.dto.BatchConfigDTO;
import com.example.demo.eand.dto.BatchProcessingRequestDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;

import com.example.demo.eand.entity.BatchJobProcessEntity;
import com.example.demo.eand.enums.BatchJobStatusEnum;
import com.example.demo.eand.enums.BatchJobTypeEnum;
import com.example.demo.eand.repo.BatchJobProcessEntityRepo;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@AllArgsConstructor
public class BatchJobPublisherServiceImpl implements BatchJobPublisherService {

    private final ObjectMapper objectMapper;
    private final BatchJobProcessEntityRepo batchJobProcessEntityRepo;
    @PersistenceContext
    private EntityManager entityManager;

    // ------------------------------------------------------------------------------------------------
    // ------------------------------------------------------------------------------------------------
    // -------------------------- Plugin Exposed methods initiateJobBatching
    // --------------------------
    // ------------------------------------------------------------------------------------------------
    // ------------------------------------------------------------------------------------------------
    // ------------------------------------------------------------------------------------------------

    /**
     * Initiate the job batching
     * 
     * @param batchConfigDTO
     */
    @Override
    public void initiateJobBatching(BatchConfigDTO batchConfigDTO) {
        log.info("BATCH JOB : Publisher - Initiating job batching with config: {}", batchConfigDTO);

        // STEP 1 : Preprocessing and prepare the batch list DTO
        List<JobBatchProcessingDto> batchList = preprocessingJobBatchTemplate(batchConfigDTO);

        // STEP 2 : Save the batch list to DB
        saveJobBatchTemplate(batchList);

        // STEP 3 : Submit the batch list to the queue
        submitJobBatchTemplate(batchList);
        log.info("BATCH JOB : Publisher - Job batching initiation completed successfully");
    }

    /**
     * 1. JobId : to identify the job and its needed parameter
     * 2. BatchId : optional: to target specific batch processing : if not provided,
     * it will process all the batches which are failed or pending
     * 3. Status : optional: to target specific batch processing type : if not
     * provided, it will process Failed batch only
     * 
     * @param dto
     */
    @Override
    public String reInitiateOldJobBatchingProcess(BatchProcessingRequestDTO dto) {

        log.info("BATCH JOB : Publisher - Re-initiating old job batching process for DTO: {}", dto);

        // STEP 1: prepare the batch list DTO from existing batch entity and mark the
        // old batch as inactive
        List<JobBatchProcessingDto> batchList = prepareDtoAndMarkOldBatchInactive(dto);

        // Validate if the batch list is not empty
        if (null == batchList || batchList.isEmpty()) {
            return "No batches found for the given criteria";
        }

        // STEP 2: save the batch entity to DB
        saveJobBatchTemplate(batchList);

        // STEP 3: Submit the batch list to the queue
        submitJobBatchTemplate(batchList);

        log.info("BATCH JOB : Publisher - Re-initiation process completed. Submitted to queue.");
        return "BATCH JOB : Publisher - Re-initiation process completed. Submitted to queue.";
    }

    // ------------------------------------------------------------------------------------------------
    // ------------------------------------------------------------------------------------------------
    // -------------------------- Private methods
    // below-----------------------------------------------
    // --------------------------------Inner
    // Working---------------------------------------------------
    // ------------------------------------------------------------------------------------------------
    // ------------------------------------------------------------------------------------------------

    /**
     * Preprocessing the batch list DTO
     * 
     * @param batchConfigDTO
     * @return List<JobBatchProcessingDto>
     */
    private List<JobBatchProcessingDto> preprocessingJobBatchTemplate(BatchConfigDTO batchConfigDTO) {
        log.info("BATCH JOB : Publisher - Starting preprocessing for job batch template");
        List<JobBatchProcessingDto> preparingBatchList = new ArrayList<>();
        String jobId = UUID.randomUUID().toString();
        Long batchId = 1L;
        long startId = 0L;
        Long endId = 0L;

        while (true) {
            JobBatchProcessingDto dto = prepareDefaultDTO(batchConfigDTO, jobId, batchId, startId);
            // TODO :Call transactional methods via an injected dependency
            List<Long> userIdsList = fetchUserIds(batchConfigDTO.getJpaSqlCommand(), startId,
                    batchConfigDTO.getBatchChunkSize());
            if (null == userIdsList || userIdsList.isEmpty()) {
                log.info("BATCH JOB : Publisher - No more records found. Stopping processing.");
                break;
            }

            endId = getLastId(userIdsList);
            dto.setEndId(endId);
            startId = endId;
            batchId++;
            preparingBatchList.add(dto);
        }

        log.info("BATCH JOB : Publisher - Preprocessing completed. Total batches prepared: {}",
                preparingBatchList.size());
        return preparingBatchList;
    }

    /**
     * Fetch the user ids from the database
     * 
     * @param sql
     * @param startId
     * @param pageSize
     * @return List<Long>
     */
    @Transactional(readOnly = true)
    public List<Long> fetchUserIds(String sql, Long startId, int pageSize) {
        log.info("BATCH JOB : Publisher - Fetching user IDs with startId: {} and pageSize: {}", startId, pageSize);
        @SuppressWarnings("unchecked")
        List<Long> rawResults = entityManager.createNativeQuery(sql)
                .setParameter("startId", startId)
                .setParameter("pageSize", pageSize)
                .getResultList();
        log.info("BATCH JOB : Publisher - Fetched {} records", rawResults != null ? rawResults.size() : 0);
        return rawResults;
    }

    /**
     * saving the batch list to DB in batch
     * 
     * @param jobBatchProcessingDto
     */
    private void saveJobBatchTemplate(List<JobBatchProcessingDto> jobBatchProcessingDto) {
        log.info("BATCH JOB : Publisher - Saving {} job batches to database", jobBatchProcessingDto.size());
        List<BatchJobProcessEntity> savedBatchEntityList = new ArrayList<>();
        jobBatchProcessingDto.forEach(batch -> {
            log.info("BATCH JOB : Publisher - Job Batch dto saving in database : {}", getWriteValueAsString(batch));
            savedBatchEntityList.add(toEntity(batch));
        });
        batchJobProcessEntityRepo.saveAll(savedBatchEntityList);
        log.info("BATCH JOB : Publisher - Successfully saved all job batches to database");
    }

    /**
     * Submit the batch list to the queue
     * 
     * @param preparingBatchList
     */
    private void submitJobBatchTemplate(List<JobBatchProcessingDto> preparingBatchList) {
        log.info("BATCH JOB : Publisher - Submitting {} job batches to queue", preparingBatchList.size());

        preparingBatchList.stream().forEach(batch -> {
            log.info("BATCH JOB : Publisher - Job Batch Template (JSON) : {}", getWriteValueAsString(batch));
            try{
                EandClient.callPostAPI(batch);
            }catch(Exception e){
                log.error("BATCH JOB : Publisher - Error in submitJobBatchTemplate: {}", e.getMessage());
            }

        });
        log.info("BATCH JOB : Publisher - All batches submitted to queue");
    }

    private List<JobBatchProcessingDto> prepareDtoAndMarkOldBatchInactive(BatchProcessingRequestDTO dto) {
        log.info("BATCH JOB : Publisher - Preparing DTO and marking old batches as inactive for JobId: {}",
                dto.getJobId());
        List<JobBatchProcessingDto> batchList = new ArrayList<>();

        // fetch the batch entity from DB
        List<BatchJobProcessEntity> savedBatchEntityList = findBatches(dto.getJobId(), dto.getBatchId(),
                dto.getStatus());

        if (null == savedBatchEntityList || savedBatchEntityList.isEmpty()) {
            log.info("BATCH JOB : Publisher - No batches found for the given criteria: {}", getWriteValueAsString(dto));
            return null;
        }

        // prepare the batch list DTO and reset the status and startedAt fields etc
        savedBatchEntityList.forEach(batch -> {

            batchList.add(JobBatchProcessingDto.builder()
                    .jobId(batch.getJobId())
                    .batchId(batch.getBatchId())
                    .startId(batch.getStartId())
                    .jobType(BatchJobTypeEnum.fromString(batch.getJobType()))
                    .status(BatchJobStatusEnum.INITIATED.name())
                    .retryCount(batch.getRetryCount() + 1)
                    .parentWorkerNode(workerNodeName())
                    // .workerNode(workerNodeName())
                    .batchChunkSize(batch.getBatchChunkSize())
                    .paginationSize(batch.getPaginationSize())
                    .executorPoolSize(batch.getExecutorPoolSize())
                    .build());

            // marked the old batch as inactive
            batch.setActiveInd(false);
            batchJobProcessEntityRepo.save(batch);
        });

        return batchList;
    }

    public List<BatchJobProcessEntity> findBatches(String jobId, Long batchId, BatchJobStatusEnum status) {

        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId is required");
        }

        if (batchId != null && status != null) {
            return batchJobProcessEntityRepo.findByJobIdAndBatchIdAndStatus(jobId, batchId, status.name());
        }

        if (batchId != null) {
            return batchJobProcessEntityRepo.findByJobIdAndBatchId(jobId, batchId);
        }

        if (status != null) {
            return batchJobProcessEntityRepo.findByJobIdAndStatus(jobId, status.name());
        }

        return batchJobProcessEntityRepo.findByJobId(jobId);
    }

    private BatchJobProcessEntity toEntity(JobBatchProcessingDto dto) {
        return BatchJobProcessEntity.builder()
                .jobId(dto.getJobId())
                .batchId(dto.getBatchId())
                .startId(dto.getStartId())
                .endId(dto.getEndId())
                .activeInd(true)
                .totalRecords(dto.getTotalRecords())
                .retryCount(dto.getRetryCount() != null ? dto.getRetryCount() : 1) // Default : 1
                .status(dto.getStatus() != null ? dto.getStatus() : BatchJobStatusEnum.INITIATED.name())
                .parentWorkerNode(dto.getParentWorkerNode())
                .retryCount(dto.getRetryCount())
                .paginationSize(dto.getPaginationSize())
                .executorPoolSize(dto.getExecutorPoolSize())
                .batchChunkSize(dto.getBatchChunkSize())
                .jobType(dto.getJobType().name())
                .build();
    }

    private JobBatchProcessingDto prepareDefaultDTO(BatchConfigDTO batchConfigDTO, String jobId, Long batchId,
            long startId) {
        return JobBatchProcessingDto.builder()
                .jobId(jobId)
                .batchId(batchId)
                .startId(startId)
                .jobType(batchConfigDTO.getJobType())
                .status(BatchJobStatusEnum.INITIATED.name())
                .retryCount(batchConfigDTO.getRetryCount())
                .parentWorkerNode(workerNodeName())
                // .workerNode(workerNodeName())
                .batchChunkSize(batchConfigDTO.getBatchChunkSize())
                .paginationSize(batchConfigDTO.getPaginationSize())
                .executorPoolSize(batchConfigDTO.getExecutorPoolSize())
                .build();
    }

    private String workerNodeName() {
        return "Parent_Worker_localhost_node";
    }

    private JobBatchProcessingDto toDTO(BatchJobProcessEntity entity) {
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

    private String getWriteValueAsString(Object batch) {
        try {
            return objectMapper.writeValueAsString(batch);
        } catch (Exception e) {
            log.error("BATCH JOB : Publisher - Error in getWriteValueAsString: {}", e.getMessage());
        }
        return "Request not parsed successfully";
    }

    protected long getLastId(List<?> records) {
        Object last = records.get(records.size() - 1);

        if (last instanceof Long) {
            return (Long) last;
        } else if (last instanceof BigInteger) {
            return ((BigInteger) last).longValue();
        }

        throw new IllegalStateException("Unsupported ID type: " + last.getClass());
    }

}
