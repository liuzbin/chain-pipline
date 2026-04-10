package com.web3.chainingestiongateway;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@MapperScan("com.web3.chainingestiongateway.mapper")
public class ChainIngestionGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChainIngestionGatewayApplication.class, args);
    }

}
