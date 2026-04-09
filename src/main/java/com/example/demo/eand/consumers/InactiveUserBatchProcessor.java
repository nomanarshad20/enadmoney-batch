package com.example.demo.eand.consumers;

import com.example.demo.eand.entity.UserEntity;
import com.example.demo.eand.enums.BatchJobTypeEnum;
import com.example.demo.eand.job.processor.AbstractBatchJobConsumerProcessor;
import com.example.demo.eand.repo.BatchJobProcessEntityRepo;
import com.example.demo.eand.repo.UserEntityRepo;
import com.example.demo.eand.service.UserServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class InactiveUserBatchProcessor extends AbstractBatchJobConsumerProcessor<UserEntity> {

    private final UserServiceImpl userServiceImpl;
    private final  UserEntityRepo userRepository;

    public InactiveUserBatchProcessor(BatchJobProcessEntityRepo batchJobProccessEntityRepo, 
                                     UserServiceImpl userServiceImpl, 
                                     UserEntityRepo userRepository,
                                     BeanFactory beanFactory,
                                     Tracer tracer) {
        super(batchJobProccessEntityRepo, beanFactory, tracer);
        this.userServiceImpl = userServiceImpl;
        this.userRepository = userRepository;
    }

    @Override
    public BatchJobTypeEnum getJobType() {
        return BatchJobTypeEnum.INACTIVE_USER;
    }

    @Override
    protected List<UserEntity> getQueryPaginatedResponse(long startingId, long endId, long pageSize) {
        return userRepository.findByIdBetweenOrderByIdAsc(startingId, endId);
    }

    @Override
    protected void processRecords(List<UserEntity> records, AtomicInteger processedCount, AtomicInteger failedCount) {
        userServiceImpl.inactiveUserMark(records, processedCount, failedCount);
    }

    @Override
    protected long getLastId(List<UserEntity> records) {
        return records.get(records.size() - 1).getId();
    }

}