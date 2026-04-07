package com.example.demo.eand.repo;

import com.example.demo.eand.entity.UserEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserEntityRepo extends JpaRepository<UserEntity, Long> {

    List<UserEntity> findByIdBetweenOrderByIdAsc(Long startId, Long endId);

    long countByIdBetween(Long startId, Long endId);
}
