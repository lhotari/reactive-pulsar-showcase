package com.github.lhotari.reactive.pulsar.showcase;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandler;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandlerBuilder;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.spring.AbstractReactiveMessageListenerContainer;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@Slf4j
public class AlarmProcessor extends AbstractReactiveMessageListenerContainer {

    public static final String ALARMPROCESSOR_DLQ_TOPIC_NAME = "alarmprocessor.dlq";
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
    protected ReactiveMessageHandler createReactiveMessageHandler() {
        log.info("Starting to consume messages");
        return ReactiveMessageHandlerBuilder
            .builder(reactivePulsarClient.messageConsumer(schema).consumerConfigurer(this::configureConsumer).build())
            .streamingMessageHandler(this::consumeMessages)
            .build();
    }

    private void configureConsumer(ConsumerBuilder<TelemetryEvent> consumerBuilder) {
        consumerBuilder
            .topic(topicNameResolver.resolveTopicName(TelemetryProcessor.TELEMETRY_MEDIAN_TOPIC_NAME))
            .subscriptionType(SubscriptionType.Key_Shared)
            .subscriptionName(getClass().getSimpleName())
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .negativeAckRedeliveryDelay(10, TimeUnit.SECONDS)
            .deadLetterPolicy(
                DeadLetterPolicy
                    .builder()
                    .deadLetterTopic(topicNameResolver.resolveTopicName(ALARMPROCESSOR_DLQ_TOPIC_NAME))
                    .maxRedeliverCount(3)
                    .build()
            );
    }

    private Flux<MessageResult<Void>> consumeMessages(Flux<Message<TelemetryEvent>> messageFlux) {
        return messageFlux.concatMap(telemetryEventMessage ->
            processMessage(telemetryEventMessage)
                .thenReturn(MessageResult.acknowledge(telemetryEventMessage.getMessageId()))
                .onErrorResume(throwable -> {
                    log.error(
                        "Error processing message, redeliveryCount {}",
                        telemetryEventMessage.getRedeliveryCount(),
                        throwable
                    );
                    return Mono.just(MessageResult.negativeAcknowledge(telemetryEventMessage.getMessageId()));
                })
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
            (
                lastSent != null &&
                lastSent.messageId.compareTo(telemetryEventMessage.getMessageId()) < 0 &&
                ALARM_THRESHOLD.compareTo(lastSent.getTelemetryEvent().getV()) !=
                ALARM_THRESHOLD.compareTo(telemetryEvent.getV())
            )
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
