package com.example.demo.eand.service;

import com.example.demo.eand.dto.BatchConfigDTO;

public interface JobBatchingPublisherService {

    void initiateJobBatching(BatchConfigDTO batchConfigDTO);

}
