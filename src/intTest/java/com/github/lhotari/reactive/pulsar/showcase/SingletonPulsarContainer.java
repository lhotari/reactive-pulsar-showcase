package com.github.lhotari.reactive.pulsar.showcase;

import java.util.UUID;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.utils.DefaultImplementation;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

public class SingletonPulsarContainer {
    public static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar")
            .withTag("2.8.0")) {
        @Override
        protected void configure() {
            super.configure();
            new WaitAllStrategy()
                    .withStrategy(waitStrategy)
                    .withStrategy(Wait.forHttp("/admin/v2/namespaces/public/default")
                            .forPort(PulsarContainer.BROKER_HTTP_PORT));
        }
    };

    static {
        PULSAR_CONTAINER.start();
    }

    public static PulsarClient createPulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(SingletonPulsarContainer.PULSAR_CONTAINER.getPulsarBrokerUrl())
                .build();
    }

    public static PulsarAdmin createPulsarAdmin() throws PulsarClientException {
        return PulsarAdmin.builder()
                .serviceHttpUrl(SingletonPulsarContainer.PULSAR_CONTAINER.getHttpServiceUrl())
                .build();
    }

    public static void registerPulsarProperties(DynamicPropertyRegistry registry) {
        registry.add("pulsar.client.serviceUrl",
                SingletonPulsarContainer.PULSAR_CONTAINER::getPulsarBrokerUrl);
    }

    public static void registerUniqueTestTopicPrefix(DynamicPropertyRegistry registry) {
        registry.add("pulsar.topicnameprefix", SingletonPulsarContainer::createUniqueTestTopicPrefix);
    }

    private static String createUniqueTestTopicPrefix() {
        try {
            String namespaceName = "public/test" + UUID.randomUUID();
            createPulsarAdmin().namespaces().createNamespace(namespaceName);
            return "persistent://" + namespaceName + "/";
        } catch (PulsarAdminException | PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

}
