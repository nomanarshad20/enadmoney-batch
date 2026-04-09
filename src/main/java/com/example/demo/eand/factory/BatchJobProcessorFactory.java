package com.example.demo.eand.factory;

import com.example.demo.eand.enums.BatchJobTypeEnum;
import com.example.demo.eand.job.processor.BatchJobProcessor;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class BatchJobProcessorFactory {
    private final Map<BatchJobTypeEnum, BatchJobProcessor> processorMap = new EnumMap<>(BatchJobTypeEnum.class);

    public BatchJobProcessorFactory(List<BatchJobProcessor> processors) {
        for (BatchJobProcessor processor : processors) {
            processorMap.put(processor.getJobType(), processor);
        }
    }

    public BatchJobProcessor getProcessor(BatchJobTypeEnum jobType) {
        BatchJobProcessor processor = processorMap.get(jobType);

        if (processor == null) {
            throw new IllegalArgumentException("No processor found for jobType: " + jobType);
        }

        return processor;
    }
}
