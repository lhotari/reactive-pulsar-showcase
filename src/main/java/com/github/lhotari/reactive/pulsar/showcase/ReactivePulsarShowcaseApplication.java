package com.github.lhotari.reactive.pulsar.showcase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
public class ReactivePulsarShowcaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactivePulsarShowcaseApplication.class, args);
    }
}
