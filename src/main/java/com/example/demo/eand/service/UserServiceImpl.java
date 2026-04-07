package com.example.demo.eand.service;

import com.example.demo.eand.dto.BatchConfigDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.entity.JobBatchProcessingEntity;
import com.example.demo.eand.entity.UserEntity;
import com.example.demo.eand.enums.BatchStatusEnum;
import com.example.demo.eand.enums.JobTypeEnum;
import com.example.demo.eand.repo.JobProcessingRepo;
import com.example.demo.eand.repo.UserEntityRepo;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@AllArgsConstructor
public class UserServiceImpl {

    private final JobProcessingTemplateService jobProcessingTemplateService;
    private final JobProcessingRepo jobProcessingRepo;

    private final UserEntityRepo userRepository;

    // SCHEDULER INITIATION PROCESS
   public String inactiveUserJobBatchTemplate() {

        // Initiate the job batching process
        BatchConfigDTO dto = BatchConfigDTO.builder()
                .jpaSqlCommand(getSQL())
                .jobType(JobTypeEnum.INACTIVE_USER)
                .retryCount(3)
                .batchChunkSize(1000)
                .paginationSize(100)
                .executorPoolSize(10)
                .build();
        jobProcessingTemplateService.initiateJobBatching(dto);

        return "publisher called";
    }


   private String getSQL(){
        String sql = """
                SELECT id
                FROM app_user
                WHERE id > :startId
                ORDER BY id ASC
                LIMIT :pageSize
                """;
        return sql;
    }








    //--------------------------------------------------------------------------------


    public void inactiveUserMark(List<UserEntity> users, AtomicInteger processedCount, AtomicInteger failedCount) {

        try {
            for (UserEntity user : users) {
                user.setStatus("INACTIVE"); // inactive
            }
            userRepository.saveAll(users);
            processedCount.addAndGet(users.size());
            log.info("Processed page successfully. recordCount={}", users.size());
        } catch (Exception ex) {
            failedCount.addAndGet(users.size());
            log.error("Failed to process page. recordCount={}", users.size(), ex);
        }
    }
}




