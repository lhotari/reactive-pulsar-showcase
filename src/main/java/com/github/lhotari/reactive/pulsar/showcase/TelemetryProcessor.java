package com.github.lhotari.reactive.pulsar.showcase;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.spring.PulsarTopicNameResolver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

@Component
@Slf4j
public class TelemetryProcessor implements SmartLifecycle {
    public static final String TELEMETRY_MEDIAN_TOPIC_NAME = "telemetry_median";
    private static final int MAX_GROUPS_IN_FLIGHT = 1000;
    private static final int MAX_GROUP_SIZE = 1000;
    private static final Duration GROUP_WINDOW_DURATION = Duration.ofSeconds(5);
    private final ReactiveMessageSender<TelemetryEntry> messageSender;
    private final ReactivePulsarClient reactivePulsarClient;
    private final Schema<TelemetryEntry> schema;
    private final AtomicBoolean running = new AtomicBoolean();
    private final PulsarTopicNameResolver topicNameResolver;
    private Disposable killSwitch;

    @Autowired
    public TelemetryProcessor(ReactivePulsarClient reactivePulsarClient,
                              ReactiveProducerCache reactiveProducerCache,
                              PulsarTopicNameResolver topicNameResolver) {
        this.topicNameResolver = topicNameResolver;
        schema = Schema.JSON(TelemetryEntry.class);
        this.messageSender = reactivePulsarClient.messageSender(schema)
                .topic(topicNameResolver.resolveTopicName(TELEMETRY_MEDIAN_TOPIC_NAME))
                .maxInflight(100)
                .cache(reactiveProducerCache)
                .create();
        this.reactivePulsarClient = reactivePulsarClient;
    }


    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting to consume messages");
            killSwitch = reactivePulsarClient.messageConsumer(schema)
                    .consumerConfigurer(this::configureConsumer)
                    .create()
                    .consumeMessages(this::consumeMessages)
                    .then()
                    .repeat()
                    .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                            .maxBackoff(Duration.ofMinutes(2)))
                    .subscribeOn(Schedulers.parallel())
                    .subscribe();
        }
    }

    private Flux<MessageResult<Void>> consumeMessages(Flux<Message<TelemetryEntry>> messageFlux) {
        return messageFlux
                .groupBy(telemetryEntryMessage -> telemetryEntryMessage.getValue().getN())
                .flatMap(group -> group
                                .publishOn(Schedulers.parallel())
                                .take(GROUP_WINDOW_DURATION)
                                .take(MAX_GROUP_SIZE)
                                .collectList()
                                .delayUntil(entriesForWindow -> processTelemetryWindow(group.key(), entriesForWindow))
                                .flatMapIterable(Function.identity())
                                .map(TelemetryProcessor::acknowledgeMessage),
                        MAX_GROUPS_IN_FLIGHT);
    }

    private Publisher<?> processTelemetryWindow(String n, List<Message<TelemetryEntry>> entriesForWindow) {
        // taking the median entry in the window can help filter out outlier values
        double median = entriesForWindow.get(entriesForWindow.size() / 2).getValue().getV();
        TelemetryEntry medianEntry = TelemetryEntry.builder().n(n).v(median).build();
        return messageSender.sendMessage(Mono.just(MessageSpec
                .builder(medianEntry)
                .key(medianEntry.getN())
                .build()));
    }

    private static MessageResult<Void> acknowledgeMessage(Message<TelemetryEntry> telemetryEntryMessage) {
        return MessageResult.acknowledge(telemetryEntryMessage.getMessageId());
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            killSwitch.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void configureConsumer(ConsumerBuilder<TelemetryEntry> consumerBuilder) {
        consumerBuilder
                .topic(topicNameResolver.resolveTopicName(IngestController.TELEMETRY_INGEST_TOPIC_NAME))
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionName(getClass().getSimpleName())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
    }
}
