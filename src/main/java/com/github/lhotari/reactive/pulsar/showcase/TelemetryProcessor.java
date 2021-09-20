package com.github.lhotari.reactive.pulsar.showcase;

import com.github.lhotari.reactive.pulsar.adapter.*;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.spring.AbstractReactiveMessageListenerContainer;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Component
@Slf4j
public class TelemetryProcessor extends AbstractReactiveMessageListenerContainer {

    public static final String TELEMETRY_MEDIAN_TOPIC_NAME = "telemetry_median";
    private static final int MAX_GROUPS_IN_FLIGHT = 1000;
    private static final int MAX_GROUP_SIZE = 1000;
    private static final Duration GROUP_WINDOW_DURATION = Duration.ofSeconds(5);
    private final ReactiveMessageSender<TelemetryEvent> messageSender;
    private final ReactivePulsarClient reactivePulsarClient;
    private final Schema<TelemetryEvent> schema;
    private final PulsarTopicNameResolver topicNameResolver;

    @Autowired
    public TelemetryProcessor(
        ReactivePulsarClient reactivePulsarClient,
        ReactiveProducerCache reactiveProducerCache,
        PulsarTopicNameResolver topicNameResolver
    ) {
        this.topicNameResolver = topicNameResolver;
        schema = Schema.JSON(TelemetryEvent.class);
        this.messageSender =
            reactivePulsarClient
                .messageSender(schema)
                .topic(topicNameResolver.resolveTopicName(TELEMETRY_MEDIAN_TOPIC_NAME))
                .maxInflight(100)
                .cache(reactiveProducerCache)
                .build();
        this.reactivePulsarClient = reactivePulsarClient;
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
            .topic(topicNameResolver.resolveTopicName(IngestController.TELEMETRY_INGEST_TOPIC_NAME))
            .subscriptionType(SubscriptionType.Key_Shared)
            .subscriptionName(getClass().getSimpleName())
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
    }

    private Flux<MessageResult<Void>> consumeMessages(Flux<Message<TelemetryEvent>> messageFlux) {
        return messageFlux
            .groupBy(telemetryEventMessage -> telemetryEventMessage.getValue().getN(), MAX_GROUPS_IN_FLIGHT)
            .flatMap(
                group ->
                    group
                        .publishOn(Schedulers.parallel())
                        .take(GROUP_WINDOW_DURATION)
                        .take(MAX_GROUP_SIZE)
                        .collectList()
                        .delayUntil(entriesForWindow -> processTelemetryWindow(group.key(), entriesForWindow))
                        .flatMapIterable(Function.identity())
                        .map(TelemetryProcessor::acknowledgeMessage),
                MAX_GROUPS_IN_FLIGHT
            );
    }

    private Publisher<?> processTelemetryWindow(String n, List<Message<TelemetryEvent>> entriesForWindow) {
        // taking the median entry in the window can help filter out outlier values
        double median = entriesForWindow.get(entriesForWindow.size() / 2).getValue().getV();
        TelemetryEvent medianEntry = TelemetryEvent.builder().n(n).v(median).build();
        return messageSender.sendMessage(Mono.just(MessageSpec.builder(medianEntry).key(medianEntry.getN()).build()));
    }

    private static MessageResult<Void> acknowledgeMessage(Message<TelemetryEvent> telemetryEventMessage) {
        return MessageResult.acknowledge(telemetryEventMessage.getMessageId());
    }
}
