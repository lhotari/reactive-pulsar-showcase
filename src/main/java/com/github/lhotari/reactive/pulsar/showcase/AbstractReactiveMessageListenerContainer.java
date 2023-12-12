package com.github.lhotari.reactive.pulsar.showcase;

import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.springframework.context.SmartLifecycle;

public abstract class AbstractReactiveMessageListenerContainer implements SmartLifecycle {

    private ReactiveMessagePipeline messagePipeline;

    protected abstract ReactiveMessagePipeline createReactiveMessagePipeline();

    @Override
    public synchronized void start() {
        if (messagePipeline == null) {
            messagePipeline = createReactiveMessagePipeline();
        }
        messagePipeline.start();
    }

    @Override
    public synchronized void stop() {
        if (messagePipeline != null) {
            messagePipeline.stop();
        }
    }

    @Override
    public synchronized boolean isRunning() {
        return messagePipeline != null ? messagePipeline.isRunning() : false;
    }
}
