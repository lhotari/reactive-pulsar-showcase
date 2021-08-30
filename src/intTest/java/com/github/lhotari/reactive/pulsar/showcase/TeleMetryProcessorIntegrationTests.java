package com.github.lhotari.reactive.pulsar.showcase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_CLASS;
import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import com.github.lhotari.reactive.pulsar.spring.test.SingletonPulsarContainer;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@SpringBootTest
@DirtiesContext(classMode = BEFORE_CLASS)
@ContextConfiguration(initializers = SingletonPulsarContainer.ContextInitializer.class)
class TeleMetryProcessorIntegrationTests {
    public static final int DEVICE_COUNT = 100;

    @Autowired
    ReactivePulsarClient reactivePulsarClient;

    @Autowired
    PulsarTopicNameResolver topicNameResolver;

    @Test
    void shouldProcessTelemetry() {
        // setup
        // create a subscription to the result topic before executing the operation
        String subscriptionName = "testSubscription" + UUID.randomUUID();
        ReactiveMessageConsumer<TelemetryEvent> messageConsumer =
                reactivePulsarClient.messageConsumer(Schema.JSON(TelemetryEvent.class))
                        .consumerConfigurer(consumerBuilder -> consumerBuilder
                                .topic(topicNameResolver.resolveTopicName(
                                        TelemetryProcessor.TELEMETRY_MEDIAN_TOPIC_NAME))
                                .subscriptionType(SubscriptionType.Exclusive)
                                .subscriptionName(subscriptionName)
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest))
                        .acknowledgeAsynchronously(false)
                        .build();
        // create the consumer and close it immediately. This is just to create the Pulsar subscription
        messageConsumer.consumeNothing().block();

        ReactiveMessageSender<TelemetryEvent> messageSender = reactivePulsarClient
                .messageSender(Schema.JSON(TelemetryEvent.class))
                .topic(topicNameResolver.resolveTopicName(IngestController.TELEMETRY_INGEST_TOPIC_NAME))
                .build();

        // when
        // 100 values for 100 devices are sent to the ingest topic
        messageSender.sendMessages(Flux.range(1, DEVICE_COUNT).flatMap(value -> {
                    String name = "device" + value + "/sensor1";
                    return Flux.range(1, 100)
                            .map(entryCounter -> TelemetryEvent.builder().n(name).v(entryCounter).build());
                }).map(telemetryEvent -> MessageSpec.builder(telemetryEvent).key(telemetryEvent.getN()).build()))
                .blockLast();

        // then the TelemetryProcessor should have aggregated a single median value for each sensor in the result topic
        Set<String> deviceNames = new HashSet<>();
        messageConsumer
                .consumeMessages(messageFlux -> messageFlux.map(
                        message -> MessageResult.acknowledge(message.getMessageId(), message)))
                .as(StepVerifier::create)
                .expectSubscription()
                .thenConsumeWhile(message -> {
                    assertThat(deviceNames.add(message.getValue().getN()))
                            .as("there shouldn't be more than 1 message per device")
                            .isTrue();
                    assertThat(message.getValue().getV())
                            .isEqualTo(51.0);
                    return deviceNames.size() < DEVICE_COUNT;
                })
                .expectNoEvent(Duration.ofSeconds(1))
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }
}