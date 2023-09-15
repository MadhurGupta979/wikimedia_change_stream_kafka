package com.dev.kafkaconsumer.repository;

import com.dev.kafkaconsumer.entity.WikimediaData;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WikimediaDataRepository extends JpaRepository<WikimediaData, Long> {
}
