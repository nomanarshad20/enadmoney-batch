package com.example.demo.eand.factory;


import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.job.processor.BatchJobProcessor;
import com.example.demo.eand.job.processor.BatchJobReInitiateProcessor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class BatchJobRoutingService {

    private final BatchJobProcessorFactory processorFactory;
    private final BatchJobReInitiateProcessor batchJobReInitiateProcessor;

    public void process(JobBatchProcessingDto jobDto) {
        BatchJobProcessor processor = processorFactory.getProcessor(jobDto.getJobType());
        processor.processBatchJob(jobDto);

        batchJobReInitiateProcessor.reInitiateFailedJobBatchingProcess(jobDto);
    }

}