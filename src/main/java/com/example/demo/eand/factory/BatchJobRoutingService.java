package com.example.demo.eand.factory;


import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.job.processor.BatchJobProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BatchJobRoutingService {

    private final BatchJobProcessorFactory processorFactory;

    public void process(JobBatchProcessingDto jobDto) {
        BatchJobProcessor processor = processorFactory.getProcessor(jobDto.getJobType());
        processor.processBatchJob(jobDto);
    }
}