package com.github.lhotari.reactive.pulsar.webhooktarget;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@Slf4j
public class WebhookTargetController {

    @PostMapping("/webhook")
    Mono<Void> handleWebhook(@RequestBody Mono<String> bodyMono, ServerHttpRequest request) {
        return bodyMono.doOnNext(body -> {
            log.info("Received webhook call with content '{}'", body);
        }).then();
    }
}
