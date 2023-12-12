package com.github.lhotari.reactive.pulsar.showcase;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * See {@link PulsarTopicNameResolver} for more information.
 */
@Component
class DefaultPulsarTopicNameResolver implements PulsarTopicNameResolver {

    private final String pulsarTopicNamePrefix;

    public DefaultPulsarTopicNameResolver(
        @Value("${pulsar.topicNamePrefix:persistent://public/default/}") String pulsarTopicNamePrefix
    ) {
        this.pulsarTopicNamePrefix = pulsarTopicNamePrefix;
    }

    @Override
    public String resolveTopicName(String topicName) {
        if (topicName.contains("://")) {
            return topicName;
        } else {
            return pulsarTopicNamePrefix + topicName;
        }
    }
}
