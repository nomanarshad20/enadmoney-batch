package com.example.demo.eand.service;

import com.example.demo.eand.dto.BatchConfigDTO;
import com.example.demo.eand.entity.UserEntity;
import com.example.demo.eand.enums.BatchJobTypeEnum;
import com.example.demo.eand.publisher.BatchJobPublisherService;
import com.example.demo.eand.repo.UserEntityRepo;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@AllArgsConstructor
public class UserServiceImpl implements UserService{

    private final BatchJobPublisherService jobProcessingTemplateService;
    private final UserEntityRepo userRepository;

    // SCHEDULER INITIATION PROCESS
   public String inactiveUserJobBatchTemplate() {

        // Initiate the job batching process
        BatchConfigDTO dto = BatchConfigDTO.builder()
                .jpaSqlCommand(getSQL())
                .jobType(BatchJobTypeEnum.INACTIVE_USER)
                .retryCount(3)
                .batchChunkSize(100000)
                .paginationSize(100000)
                .executorPoolSize(1)
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


    public void inactiveUserMark(List<UserEntity> users, AtomicInteger processedCount, AtomicInteger failedCount ,String jobId, Long batchId) {
        try {
            for (UserEntity user : users) {

            /*    if( user.getId() == 5500){
                    log.error("test -  exception behavior jobId={} batchId={}" , jobId , batchId);
                   // throw new RuntimeException("test -  exception behavior");
                }

                if(user.getId() == 7000 ){
                    log.error("test -  exception behavior jobId={} batchId={}" , jobId , batchId);
                    failedCount.incrementAndGet();
                    continue;
                }*/


                user.setStatus("INACTIVE"); // inactive
                userRepository.save(user);
                processedCount.incrementAndGet();
            }
            log.info("Processed page successfully. recordCount={}", users.size());
        } catch (Exception ex) {
            failedCount.incrementAndGet();
            log.error("Failed to process page. recordCount={}", users.size(), ex);
            throw new RuntimeException("test -  exception behavior");
        }
    }



}




