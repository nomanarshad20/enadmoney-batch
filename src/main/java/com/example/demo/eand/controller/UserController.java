package com.example.demo.eand.controller;


import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.factory.BatchJobRoutingService;
import com.example.demo.eand.pc.ConsumerClient;
import com.example.demo.eand.service.UserServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserServiceImpl appUserService;

    private final ConsumerClient consumerClient;

    private final BatchJobRoutingService batchJobRoutingService;


    @GetMapping("/job-batching/publisher")
    public ResponseEntity<?> consume() {
        return ResponseEntity.ok(appUserService.inactiveUserJobBatchTemplate());
    }


    @PostMapping("/job-batching/consumer")
    public ResponseEntity<?> createUser(@RequestBody JobBatchProcessingDto dto) {
        batchJobRoutingService.process(dto);
        return ResponseEntity.ok("SUCCESS");
    }



}
