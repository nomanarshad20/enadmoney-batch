package com.example.demo.eand.client;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;



public class EandClient {


   public static boolean callPostAPI(JobBatchProcessingDto request) {

        String url = "http://localhost:8181/api/users/consumer";

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ObjectMapper objectMapper = new ObjectMapper();
        String json = null;
       try{
           json=    objectMapper.writeValueAsString(request);
       }catch (Exception e){
            System.out.println(e.getMessage());
       }

        HttpEntity<String> entity = new HttpEntity<>(json, headers);

        ResponseEntity<String> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                String.class
        );

        System.out.println("Response: " + response.getBody());
        return true;
    }
}

