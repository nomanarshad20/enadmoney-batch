package com.example.demo.eand.controller;


import com.example.demo.eand.dto.BatchProcessingRequestDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.factory.BatchJobRoutingService;
import com.example.demo.eand.service.UserServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserServiceImpl appUserService;

    private final BatchJobRoutingService batchJobRoutingService;


    @GetMapping("/publisher")
    public ResponseEntity<?> consume() {
        return ResponseEntity.ok(appUserService.inactiveUserJobBatchTemplate());
    }








    // Today :
    // re initiate failed or pending batch jobs by api.: submit to quee
    // retry failed batch jobs after finishing the current job.
    // re initiate current batch job if failed
    // performance testing with different batch size and number of jobs
    // implement tread span id for tracing purpose

}
