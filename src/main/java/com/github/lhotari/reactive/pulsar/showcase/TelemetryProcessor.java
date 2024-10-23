package com.github.lhotari.reactive.pulsar.showcase;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Component
@Slf4j
public class TelemetryProcessor extends AbstractReactiveMessagePipelineContainer {

    public static final String TELEMETRY_MEDIAN_TOPIC_NAME = "telemetry_median";
    private static final int MAX_GROUPS_IN_FLIGHT = 5000;
    private static final int MAX_GROUP_SIZE = 1000;
    private static final Duration GROUP_WINDOW_DURATION = Duration.ofSeconds(2);
    private final ReactiveMessageSender<TelemetryEvent> messageSender;
    private final ReactivePulsarClient reactivePulsarClient;
    private final Schema<TelemetryEvent> schema;
    private final PulsarTopicNameResolver topicNameResolver;

    @Autowired
    public TelemetryProcessor(
        ReactivePulsarClient reactivePulsarClient,
        ReactiveMessageSenderCache reactiveMessageSenderCache,
        PulsarTopicNameResolver topicNameResolver
    ) {
        this.topicNameResolver = topicNameResolver;
        schema = Schema.JSON(TelemetryEvent.class);
        this.messageSender = reactivePulsarClient
            .messageSender(schema)
            .topic(topicNameResolver.resolveTopicName(TELEMETRY_MEDIAN_TOPIC_NAME))
            .maxInflight(100)
            .cache(reactiveMessageSenderCache)
            .maxConcurrentSenderSubscriptions(10000)
            .build();
        this.reactivePulsarClient = reactivePulsarClient;
    }

    @Override
    protected ReactiveMessagePipeline createReactiveMessagePipeline() {
        log.info("Starting to consume messages");
        return configureConsumer(reactivePulsarClient.messageConsumer(schema))
            .build()
            .messagePipeline()
            .streamingMessageHandler(this::consumeMessages)
            .build();
    }

    private ReactiveMessageConsumerBuilder<TelemetryEvent> configureConsumer(
        ReactiveMessageConsumerBuilder<TelemetryEvent> consumerBuilder
    ) {
        return consumerBuilder
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
        return messageSender.sendOne(MessageSpec.builder(medianEntry).key(medianEntry.getN()).build());
    }

    private static MessageResult<Void> acknowledgeMessage(Message<TelemetryEvent> telemetryEventMessage) {
        return MessageResult.acknowledge(telemetryEventMessage.getMessageId());
    }
}
