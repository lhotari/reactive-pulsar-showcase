package com.github.lhotari.reactive.pulsar.showcase;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(builder = TelemetryEvent.TelemetryEventBuilder.class)
@AllArgsConstructor
public class TelemetryEvent {

    String n;
    double v;

    @JsonPOJOBuilder(withPrefix = "")
    public static class TelemetryEventBuilder {}
}
