package com.web3.extractor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ExtractorApplication {
    public static void main(String[] args) {
        SpringApplication.run(ExtractorApplication.class, args);
    }
}