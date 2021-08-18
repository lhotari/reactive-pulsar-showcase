package com.github.lhotari.reactive.pulsar.showcase;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import lombok.Builder;
import lombok.Value;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class IngestController {
    private final ReactiveMessageSender<TelemetryEntry> messageSender;

    @Autowired
    public IngestController(ReactivePulsarClient reactivePulsarClient,
                            ReactiveProducerCache reactiveProducerCache) {
        this(reactivePulsarClient.messageSender(Schema.JSON(TelemetryEntry.class))
                .topic("telemetry_ingest")
                .maxInflight(100)
                .cache(reactiveProducerCache)
                .create());
    }

    IngestController(ReactiveMessageSender<TelemetryEntry> messageSender) {
        this.messageSender = messageSender;
    }

    @PostMapping("/telemetry")
    Mono<Void> ingest(Flux<TelemetryEntry> telemetryEntryFlux) {
        return messageSender
                .sendMessages(telemetryEntryFlux.map(MessageSpec::of))
                .then();
    }

    @Value
    @Builder
    @JsonDeserialize(builder = TelemetryEntry.TelemetryEntryBuilder.class)
    public static class TelemetryEntry {
        String n;
        double v;
    }
}
