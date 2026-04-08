package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.entity.UserEntity;
import com.example.demo.eand.enums.JobTypeEnum;
import com.example.demo.eand.repo.JobProcessingRepo;
import com.example.demo.eand.repo.UserEntityRepo;
import com.example.demo.eand.service.UserServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class InactiveUserBatchProcessor extends AbstractBatchJobProcessor<UserEntity> {

    private final UserServiceImpl userServiceImpl;
    private final  UserEntityRepo userRepository;

    public InactiveUserBatchProcessor(JobProcessingRepo jobProcessingRepo, UserServiceImpl userServiceImpl, UserEntityRepo userRepository) {
        super(jobProcessingRepo);
        this.userServiceImpl = userServiceImpl;
        this.userRepository = userRepository;
    }

    @Override
    public JobTypeEnum getJobType() {
        return JobTypeEnum.INACTIVE_USER;
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

    @Override
    protected int countRecords(JobBatchProcessingDto dto) {
        return (int) userRepository.countByIdBetween(dto.getStartId(), dto.getEndId());
    }


}