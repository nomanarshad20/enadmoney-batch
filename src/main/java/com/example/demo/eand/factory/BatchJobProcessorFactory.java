package com.example.demo.eand.factory;

import com.example.demo.eand.enums.JobTypeEnum;
import com.example.demo.eand.job.processor.BatchJobProcessor;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class BatchJobProcessorFactory {
    private final Map<JobTypeEnum, BatchJobProcessor> processorMap = new EnumMap<>(JobTypeEnum.class);

    public BatchJobProcessorFactory(List<BatchJobProcessor> processors) {
        for (BatchJobProcessor processor : processors) {
            processorMap.put(processor.getJobType(), processor);
        }
    }

    public BatchJobProcessor getProcessor(JobTypeEnum jobType) {
        BatchJobProcessor processor = processorMap.get(jobType);

        if (processor == null) {
            throw new IllegalArgumentException("No processor found for jobType: " + jobType);
        }

        return processor;
    }
}
