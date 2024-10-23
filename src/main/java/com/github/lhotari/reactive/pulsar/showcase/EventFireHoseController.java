package com.github.lhotari.reactive.pulsar.showcase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.EndOfStreamAction;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderBuilder;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.apache.pulsar.reactive.client.api.StartAtSpec;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class EventFireHoseController {

    private static final Duration KEEPALIVE_INTERVAL = Duration.ofSeconds(10);
    private final ReactiveMessageReaderBuilder<TelemetryEvent> messageReaderBuilderTemplate;
    private final PulsarTopicNameResolver topicNameResolver;

    public EventFireHoseController(
        ReactivePulsarClient reactivePulsarClient,
        PulsarTopicNameResolver topicNameResolver
    ) {
        this.topicNameResolver = topicNameResolver;
        messageReaderBuilderTemplate = reactivePulsarClient
            .messageReader(Schema.JSON(TelemetryEvent.class))
            .topic(topicNameResolver.resolveTopicName(IngestController.TELEMETRY_INGEST_TOPIC_NAME))
            .startAtSpec(StartAtSpec.ofLatestInclusive())
            .endOfStreamAction(EndOfStreamAction.POLL);
    }

    @CrossOrigin(allowedHeaders = "*")
    @GetMapping(path = "/firehose/{source}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<ServerSentEvent<TelemetryEvent>> streamEvents(
        @RequestHeader(value = "Last-Event-ID", required = false) Optional<String> lastEventIDHeader,
        @RequestParam(value = "lastEventId", required = false) Optional<String> lastEventIDParameter,
        @PathVariable(value = "source", required = false) Optional<String> source,
        @RequestParam(value = "poll", defaultValue = "true") boolean pollMore
    ) {
        ReactiveMessageReaderBuilder<TelemetryEvent> messageReaderBuilder = messageReaderBuilderTemplate.clone();

        if (!pollMore) {
            messageReaderBuilder.endOfStreamAction(EndOfStreamAction.COMPLETE);
        }

        source.ifPresent(s -> {
            if (s.equals("median")) {
                messageReaderBuilder.topic(
                    topicNameResolver.resolveTopicName(TelemetryProcessor.TELEMETRY_MEDIAN_TOPIC_NAME)
                );
            }
        });

        // support resuming the event stream using SSE's Last-Event-ID header or lastEventId query parameter
        // lastEventId query parameter is used as a CORS limitation workaround
        (lastEventIDHeader.isPresent() ? lastEventIDHeader : lastEventIDParameter).ifPresent(id -> {
                try {
                    // this is not a secure solution for production solutions.
                    // Consider using a HMAC to prevent tampering the message id
                    MessageId lastMessageId = MessageId.fromByteArray(
                        Base64.getUrlDecoder().decode(id.getBytes(StandardCharsets.UTF_8))
                    );
                    messageReaderBuilder.startAtSpec(StartAtSpec.ofMessageId(lastMessageId, false));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

        return Flux.merge(
            messageReaderBuilder
                .build()
                .readMany()
                .map(telemetryEventMessage ->
                    ServerSentEvent.builder(telemetryEventMessage.getValue())
                        .event("telemetry")
                        .id(Base64.getUrlEncoder().encodeToString(telemetryEventMessage.getMessageId().toByteArray()))
                        .build()
                ),
            pollMore ? createKeepaliveFlux() : Flux.empty()
        );
    }

    private Flux<ServerSentEvent<TelemetryEvent>> createKeepaliveFlux() {
        return Flux.interval(KEEPALIVE_INTERVAL).map(i -> ServerSentEvent.<TelemetryEvent>builder().comment("").build()
        );
    }
}
