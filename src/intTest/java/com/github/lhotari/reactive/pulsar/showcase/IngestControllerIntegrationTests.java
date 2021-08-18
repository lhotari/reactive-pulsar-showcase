package com.github.lhotari.reactive.pulsar.showcase;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageReader;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import java.time.Duration;
import java.util.UUID;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

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
        ReactiveMessageConsumer<IngestController.TelemetryEntry> messageConsumer =
                reactivePulsarClient.messageConsumer(Schema.JSON(IngestController.TelemetryEntry.class))
                        .consumerConfigurer(consumerBuilder -> consumerBuilder
                                .topic(IngestController.TELEMETRY_INGEST_TOPIC_NAME)
                                .subscriptionName("testSubscription" + UUID.randomUUID())
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest))
                        .create();
        // create the consumer and close it immediately. This is just to create the Pulsar subscription
        messageConsumer.consumeMessage()
                .timeout(Duration.ofSeconds(1), Mono.empty())
                .block();

        webTestClient.post().uri("/telemetry")
                .contentType(MediaType.APPLICATION_NDJSON)
                .bodyValue("{\"n\": \"device1\", \"v\": 1.23}")
                .exchange()
                .expectStatus().isOk();

        ReactiveMessageReader<IngestController.TelemetryEntry> reactiveMessageReader =
                reactivePulsarClient.messageReader(Schema.JSON(IngestController.TelemetryEntry.class))
                        .topic(IngestController.TELEMETRY_INGEST_TOPIC_NAME)
                        .create();

        Message<IngestController.TelemetryEntry> telemetryEntryMessage =
                reactiveMessageReader.readMessage().block(Duration.ofSeconds(5));
        assertThat(telemetryEntryMessage.getValue()).isEqualTo(IngestController.TelemetryEntry.builder()
                .n("device1")
                .v(1.23)
                .build());
    }
}