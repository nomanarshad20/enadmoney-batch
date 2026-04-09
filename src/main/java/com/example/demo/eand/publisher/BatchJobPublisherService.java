package com.example.demo.eand.publisher;

import com.example.demo.eand.dto.BatchConfigDTO;
import com.example.demo.eand.dto.BatchProcessingRequestDTO;

public interface BatchJobPublisherService {

    void initiateJobBatching(BatchConfigDTO batchConfigDTO);

    String reInitiateOldJobBatchingProcess(BatchProcessingRequestDTO dto);

}
