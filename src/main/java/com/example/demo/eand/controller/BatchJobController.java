package com.example.demo.eand.controller;

import com.example.demo.eand.dto.BatchProcessingRequestDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.factory.BatchJobRoutingService;
import com.example.demo.eand.publisher.BatchJobPublisherService;
import com.example.demo.eand.service.UserServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/batch-job")
@RequiredArgsConstructor
public class BatchJobController {


    private final BatchJobRoutingService batchJobRoutingService;
    private final BatchJobPublisherService batchJobPublisherService;

    /**
     * 1. JobId : to identify the job and its needed parameter
     * 2. BatchId : optional: to target specific batch processing : if not provided, it will process all the batches which are failed or pending
     * 3. Status : optional: to target specific batch processing type : if not provided, it will process Failed batch only
     */
    @PostMapping("/publisher/initiate")
    public ResponseEntity<?> reInitiateBatchJobs(@RequestBody BatchProcessingRequestDTO dto) {
        batchJobPublisherService.reInitiateOldJobBatchingProcess(dto);
        return ResponseEntity.ok("SUCCESS");
    }



    @PostMapping("/consumer")
    public ResponseEntity<?> createUser(@RequestBody JobBatchProcessingDto dto) {
        batchJobRoutingService.process(dto);
        return ResponseEntity.ok("SUCCESS");
    }

}
