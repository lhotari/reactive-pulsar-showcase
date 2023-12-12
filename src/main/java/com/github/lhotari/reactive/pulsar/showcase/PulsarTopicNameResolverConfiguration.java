package com.github.lhotari.reactive.pulsar.showcase;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarTopicNameResolverConfiguration {

    @Bean
    @ConditionalOnMissingBean
    PulsarTopicNameResolver pulsarTopicNameResolver(
        @Value("${pulsar.topicNamePrefix:persistent://public/default/}") String pulsarTopicPrefix
    ) {
        return new DefaultPulsarTopicNameResolver(pulsarTopicPrefix);
    }
}
