package com.github.lhotari.reactive.pulsar.showcase;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(builder = TelemetryEntry.TelemetryEntryBuilder.class)
@AllArgsConstructor
public class TelemetryEntry {
    String n;
    double v;

    @JsonPOJOBuilder(withPrefix = "")
    public static class TelemetryEntryBuilder {
    }
}
