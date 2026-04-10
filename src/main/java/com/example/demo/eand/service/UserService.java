package com.example.demo.eand.service;

import com.example.demo.eand.entity.UserEntity;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public interface UserService {

    public void inactiveUserMark(List<UserEntity> users, AtomicInteger processedCount, AtomicInteger failedCount , String jobId, Long batchId);
}
