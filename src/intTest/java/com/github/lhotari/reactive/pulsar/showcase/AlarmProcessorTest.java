package com.github.lhotari.reactive.pulsar.showcase;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import com.github.lhotari.reactive.pulsar.spring.test.SingletonPulsarContainer;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.cloud.contract.wiremock.WireMockConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "alarmwebhook.url=http://localhost:${wiremock.server.port}/webhook"
)
@ContextConfiguration(initializers = SingletonPulsarContainer.ContextInitializer.class)
@AutoConfigureWireMock(port = 0)
@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
class AlarmProcessorTest {

    @Autowired
    ReactivePulsarClient reactivePulsarClient;

    @Autowired
    PulsarTopicNameResolver topicNameResolver;

    @Autowired
    WebhookResponseHolder webhookResponseHolder;

    @Value
    static class WebhookResponseHolder {

        private final BlockingQueue<TelemetryEvent> responses = new ArrayBlockingQueue<>(100);

        public void add(TelemetryEvent telemetryEvent) {
            responses.add(telemetryEvent);
        }

        public TelemetryEvent waitForResponse() throws InterruptedException {
            return responses.poll(5, TimeUnit.SECONDS);
        }

        public List<TelemetryEvent> waitForResponses(int numberOfResponses) throws InterruptedException {
            List<TelemetryEvent> results = new ArrayList<>(numberOfResponses);
            while (results.size() < numberOfResponses) {
                TelemetryEvent telemetryEvent = waitForResponse();
                if (telemetryEvent == null) {
                    break;
                }
                results.add(telemetryEvent);
            }
            return results;
        }
    }

    static class WebhookResponseTransformer extends ResponseTransformer {

        static final String WEBHOOK_TRANSFORMER_NAME = "webhook";
        private final ObjectMapper objectMapper;
        private final WebhookResponseHolder webhookResponseHolder;

        public WebhookResponseTransformer(ObjectMapper objectMapper, WebhookResponseHolder webhookResponseHolder) {
            this.objectMapper = objectMapper;
            this.webhookResponseHolder = webhookResponseHolder;
        }

        @Override
        public Response transform(Request request, Response response, FileSource files, Parameters parameters) {
            try {
                TelemetryEvent telemetryEvent = objectMapper.readValue(request.getBodyAsString(), TelemetryEvent.class);
                webhookResponseHolder.add(telemetryEvent);
                return Response.response().status(200).build();
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize request body to TelemetryEvent", e);
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String getName() {
            return WEBHOOK_TRANSFORMER_NAME;
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }
    }

    @TestConfiguration
    static class CustomConfig {

        @Bean
        WebhookResponseHolder webhookResponseHolder() {
            return new WebhookResponseHolder();
        }

        @Bean
        WireMockConfigurationCustomizer optionsCustomizer(
            WebhookResponseHolder webhookResponseHolder,
            ObjectMapper objectMapper
        ) {
            return config -> {
                config.disableRequestJournal();
                config.extensions(new WebhookResponseTransformer(objectMapper, webhookResponseHolder));
            };
        }
    }

    @Test
    void shouldDeliverEventWhenValueGoesOverThresholdAndWhenItGoesBackToNormal() throws InterruptedException {
        // given
        stubFor(
            post(urlEqualTo("/webhook"))
                .willReturn(aResponse().withTransformers(WebhookResponseTransformer.WEBHOOK_TRANSFORMER_NAME))
        );
        ReactiveMessageSender<TelemetryEvent> messageSender = reactivePulsarClient
            .messageSender(Schema.JSON(TelemetryEvent.class))
            .topic(topicNameResolver.resolveTopicName(TelemetryProcessor.TELEMETRY_MEDIAN_TOPIC_NAME))
            .build();

        // when telemetry events are sent
        messageSender
            .sendMessages(
                Flux
                    .just(1.0d, 2.0d, 81.0d, 120.0d, 66.0d, 75.0d, 73.0d, 82.0d)
                    .map(value -> new TelemetryEvent("device1/sensor1", value))
                    .map(telemetryEvent -> MessageSpec.builder(telemetryEvent).key(telemetryEvent.getN()).build())
            )
            .blockLast();

        // then
        List<TelemetryEvent> receivedAlarmEvents = webhookResponseHolder.waitForResponses(3);
        assertThat(receivedAlarmEvents).extracting(TelemetryEvent::getV).containsExactly(81.0d, 66.0d, 82.0d);
    }
}
