package com.github.lhotari.reactive.pulsar.showcase;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import java.time.Duration;
import java.util.UUID;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IngestControllerIntegrationTests {
    @DynamicPropertySource
    static void registerPulsarProperties(DynamicPropertyRegistry registry) {
        SingletonPulsarContainer.registerPulsarProperties(registry);
    }

    @Autowired
    ReactivePulsarClient reactivePulsarClient;

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void shouldIngestTelemetry() {
        // setup
        // create a subscription to the result topic before executing the operation
        String subscriptionName = "testSubscription" + UUID.randomUUID();
        ReactiveMessageConsumer<TelemetryEntry> messageConsumer =
                reactivePulsarClient.messageConsumer(Schema.JSON(TelemetryEntry.class))
                        .consumerConfigurer(consumerBuilder -> consumerBuilder
                                .topic(IngestController.TELEMETRY_INGEST_TOPIC_NAME)
                                .subscriptionType(SubscriptionType.Exclusive)
                                .subscriptionName(subscriptionName)
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest))
                        .acknowledgeAsynchronously(false)
                        .create();
        // create the consumer and close it immediately. This is just to create the Pulsar subscription
        messageConsumer.consumeNothing().block();

        // when
        webTestClient.post().uri("/telemetry")
                .contentType(MediaType.APPLICATION_NDJSON)
                .bodyValue("{\"n\": \"device1\", \"v\": 1.23}\n{\"n\": \"device2\", \"v\": 3.21}")
                .exchange()
                .expectStatus().isOk();

        // then
        messageConsumer
                .consumeMessages(messageFlux -> messageFlux.map(
                        message -> MessageResult.acknowledge(message.getMessageId(), message)))
                .as(StepVerifier::create)
                .expectSubscription()
                .assertNext(telemetryEntryMessage ->
                        assertThat(telemetryEntryMessage.getValue())
                                .isEqualTo(TelemetryEntry.builder()
                                        .n("device1")
                                        .v(1.23)
                                        .build()))
                .assertNext(telemetryEntryMessage ->
                        assertThat(telemetryEntryMessage.getValue())
                                .isEqualTo(TelemetryEntry.builder()
                                        .n("device2")
                                        .v(3.21)
                                        .build()))
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }
}