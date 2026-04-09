package com.example.demo.eand.dto;

import com.example.demo.eand.enums.BatchJobTypeEnum;
import lombok.*;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BatchConfigDTO {
    private String jpaSqlCommand;
    private BatchJobTypeEnum jobType;
    private Integer retryCount;
    private Integer batchChunkSize;
    private Integer paginationSize;
    private Integer executorPoolSize;
}