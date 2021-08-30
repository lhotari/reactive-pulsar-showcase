package com.github.lhotari.reactive.pulsar.showcase;

import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@Slf4j
public class IngestController {
    public static final String TELEMETRY_INGEST_TOPIC_NAME = "telemetry_ingest";
    private final ReactiveMessageSender<TelemetryEvent> messageSender;

    @Autowired
    public IngestController(ReactivePulsarClient reactivePulsarClient,
                            ReactiveProducerCache reactiveProducerCache,
                            PulsarTopicNameResolver topicNameResolver) {
        this(reactivePulsarClient.messageSender(Schema.JSON(TelemetryEvent.class))
                .topic(topicNameResolver.resolveTopicName(TELEMETRY_INGEST_TOPIC_NAME))
                .maxInflight(100)
                .cache(reactiveProducerCache)
                .build());
    }

    IngestController(ReactiveMessageSender<TelemetryEvent> messageSender) {
        this.messageSender = messageSender;
    }

    @PostMapping("/telemetry")
    Mono<Void> ingest(@RequestBody Flux<TelemetryEvent> telemetryEventFlux) {
        return messageSender
                .sendMessages(telemetryEventFlux
                        .doOnNext(telemetryEvent -> {
                            log.info("About to send telemetry entry {}", telemetryEvent);
                        })
                        .map(telemetryEvent -> MessageSpec.builder(telemetryEvent)
                                .key(telemetryEvent.getN())
                                .build()))
                .then();
    }

}
