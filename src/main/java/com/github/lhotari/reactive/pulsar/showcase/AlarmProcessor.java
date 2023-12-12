package com.github.lhotari.reactive.pulsar.showcase;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@Slf4j
public class AlarmProcessor extends AbstractReactiveMessageListenerContainer {

    public static final String ALARMPROCESSOR_DLQ_TOPIC_NAME = "alarmprocessor.dlq";
    public static final int MAX_CONCURRENCY = 32;
    private final ReactivePulsarClient reactivePulsarClient;
    private final Schema<TelemetryEvent> schema;
    private final PulsarTopicNameResolver topicNameResolver;
    private final ConcurrentHashMap<String, TelemetryLastSent> lastSentState = new ConcurrentHashMap<>();
    private final WebClient webhookWebclient;
    private final Double ALARM_THRESHOLD = 80.0d;

    @Value
    private static class TelemetryLastSent {

        MessageId messageId;
        TelemetryEvent telemetryEvent;
    }

    @Autowired
    public AlarmProcessor(
        ReactivePulsarClient reactivePulsarClient,
        PulsarTopicNameResolver topicNameResolver,
        WebClient.Builder webClientBuilder,
        @org.springframework.beans.factory.annotation.Value(
            "${alarmwebhook.url:http://localhost:8082/webhook}"
        ) String alarmWebhookUrl
    ) {
        this.topicNameResolver = topicNameResolver;
        schema = Schema.JSON(TelemetryEvent.class);
        this.reactivePulsarClient = reactivePulsarClient;
        this.webhookWebclient = webClientBuilder.baseUrl(alarmWebhookUrl).build();
    }

    @Override
    protected ReactiveMessagePipeline createReactiveMessagePipeline() {
        log.info("Starting to consume messages");
        return configureConsumer(reactivePulsarClient.messageConsumer(schema))
            .build()
            .messagePipeline()
            .messageHandler(this::processMessage)
            .concurrency(MAX_CONCURRENCY)
            .useKeyOrderedProcessing()
            .build();
    }

    private ReactiveMessageConsumerBuilder<TelemetryEvent> configureConsumer(
        ReactiveMessageConsumerBuilder<TelemetryEvent> consumerBuilder
    ) {
        return consumerBuilder
            .topic(topicNameResolver.resolveTopicName(TelemetryProcessor.TELEMETRY_MEDIAN_TOPIC_NAME))
            .subscriptionType(SubscriptionType.Key_Shared)
            .subscriptionName(getClass().getSimpleName())
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .negativeAckRedeliveryDelay(Duration.ofSeconds(10))
            .deadLetterPolicy(
                DeadLetterPolicy
                    .builder()
                    .deadLetterTopic(topicNameResolver.resolveTopicName(ALARMPROCESSOR_DLQ_TOPIC_NAME))
                    .maxRedeliverCount(3)
                    .build()
            );
    }

    private Mono<Void> processMessage(Message<TelemetryEvent> telemetryEventMessage) {
        if (hasLastSentStateChanged(telemetryEventMessage)) {
            return sendStateToWebhook(telemetryEventMessage);
        } else {
            return Mono.empty();
        }
    }

    private Mono<Void> sendStateToWebhook(Message<TelemetryEvent> telemetryEventMessage) {
        return webhookWebclient
            .post()
            .bodyValue(telemetryEventMessage.getValue())
            .retrieve()
            .toBodilessEntity()
            .then(updateLastSentState(telemetryEventMessage))
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)));
    }

    private boolean hasLastSentStateChanged(Message<TelemetryEvent> telemetryEventMessage) {
        TelemetryEvent telemetryEvent = telemetryEventMessage.getValue();
        TelemetryLastSent lastSent = lastSentState.get(telemetryEvent.getN());
        return (
            (lastSent == null && telemetryEvent.getV() > ALARM_THRESHOLD) ||
            (lastSent != null &&
                lastSent.messageId.compareTo(telemetryEventMessage.getMessageId()) < 0 &&
                ALARM_THRESHOLD.compareTo(lastSent.getTelemetryEvent().getV()) !=
                    ALARM_THRESHOLD.compareTo(telemetryEvent.getV()))
        );
    }

    private Mono<Void> updateLastSentState(Message<TelemetryEvent> telemetryEventMessage) {
        return Mono.fromRunnable(() -> {
            TelemetryEvent telemetryEvent = telemetryEventMessage.getValue();
            lastSentState.put(
                telemetryEvent.getN(),
                new TelemetryLastSent(telemetryEventMessage.getMessageId(), telemetryEvent)
            );
        });
    }
}
