package com.example.demo.eand.factory;


import com.example.demo.eand.dto.BatchProcessingRequestDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.job.processor.BatchJobProcessor;
import com.example.demo.eand.job.processor.BatchJobReInitiateProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BatchJobRoutingService {

    private final BatchJobProcessorFactory processorFactory;
    private final BatchJobReInitiateProcessor batchJobReInitiateProcessor;

    public void process(JobBatchProcessingDto jobDto) {
        BatchJobProcessor processor = processorFactory.getProcessor(jobDto.getJobType());
        processor.processBatchJob(jobDto);
        batchJobReInitiateProcessor.reInitiateFailedJobBatchingProcess(jobDto);
    }

}