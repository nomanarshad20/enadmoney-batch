package com.example.demo.eand.dto;


import com.example.demo.eand.enums.BatchJobStatusEnum;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BatchProcessingRequestDTO {

  //  @NonEmpty("JobId should not be null or empty")
    private String jobId; // Must: to identify the job
    private Long batchId; // optional: to target specific batch processing
    private BatchJobStatusEnum status; // optional: to target specific batch processing type : if not provided, it will process Failed batch only
}
