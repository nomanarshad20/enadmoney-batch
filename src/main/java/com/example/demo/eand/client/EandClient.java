package com.example.demo.eand.client;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

@Slf4j
public class EandClient {

    @Value("${eand.batch.job.base.url}")
    private String batchJobBaseUrl;

    @Value("${eand.batch.job.reinitiate.url}")
    private String batchJobReInitiateUrl;

    public static boolean callPostAPI(JobBatchProcessingDto request) {

        String url = "http://localhost:8181/api/batch-job/consumer";

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ObjectMapper objectMapper = new ObjectMapper();
        String json = null;
        try {
            json = objectMapper.writeValueAsString(request);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        HttpEntity<String> entity = new HttpEntity<>(json, headers);

        ResponseEntity<String> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                String.class
        );

        log.info("Response: {}", response.getBody());
        return true;
    }


    public boolean callPublisherPostAPI(JobBatchProcessingDto request) {

        String url = batchJobBaseUrl + batchJobReInitiateUrl;

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ObjectMapper objectMapper = new ObjectMapper();
        String json = null;
        try {
            json = objectMapper.writeValueAsString(request);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        HttpEntity<String> entity = new HttpEntity<>(json, headers);

        ResponseEntity<String> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                String.class
        );

        log.info("Response: {}", response.getBody());
        return true;
    }


}

