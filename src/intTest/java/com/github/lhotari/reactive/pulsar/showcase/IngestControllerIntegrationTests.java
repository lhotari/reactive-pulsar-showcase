package com.github.lhotari.reactive.pulsar.showcase;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import java.time.Duration;
import java.util.UUID;
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
                                .topic("telemetry")
                                .subscriptionName("testSubscription" + UUID.randomUUID())
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest))
                        .create();
        // create the consumer and close it immediately. This is just to create the Pulsar subscription
        messageConsumer.consumeMessage()
                .timeout(Duration.ofMillis(1), Mono.empty())
                .block();

        webTestClient.post().uri("/telemetry")
                .contentType(MediaType.APPLICATION_NDJSON)
                .bodyValue("{\"n\": \"1.23\"}")
                .exchange()
                .expectStatus().isOk();

        messageConsumer.consumeMessage()
                // TODO: this won't work, reported as https://github.com/lhotari/reactive-pulsar/issues/1
                //.doOnNext(ConsumedMessage::acknowledge)
                .block(Duration.ofSeconds(10));
    }
}