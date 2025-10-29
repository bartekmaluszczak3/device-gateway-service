package com.example.device.gateway.web;

import com.example.device.gateway.Application;
import com.example.device.gateway.kafka.KafkaConsumer;
import com.example.device.gateway.service.MqttTelemetryListener;
import lombok.SneakyThrows;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@SpringBootTest(classes = Application.class)
@Testcontainers
class MqttTelemetryListenerTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Container
    static GenericContainer<?> mosquitto = new GenericContainer<>(DockerImageName.parse("eclipse-mosquitto:2.0"))
            .withExposedPorts(1883)
            .withCommand("/usr/sbin/mosquitto -c /mosquitto-no-auth.conf")
            .waitingFor(Wait.forLogMessage(".*mosquitto version.*running.*\\n", 1));

    @Autowired
    private MqttTelemetryListener mqttListener;

    @Autowired
    private KafkaConsumer kafkaConsumer;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("service.mqtt.broker-url",
                () -> "tcp://" + mosquitto.getHost() + ":" + mosquitto.getMappedPort(1883));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @SneakyThrows
    @Test
    void shouldForwardMqttMessageToKafka() {
        String mqttHost = mosquitto.getHost();
        int mqttPort = mosquitto.getMappedPort(1883);
        String topic = "iot/device123/telemetry";
        String payload = "{\"temp\": 25.3}";

        MqttClient publisher = new MqttClient("tcp://" + mqttHost + ":" + mqttPort, "test-publisher");
        publisher.connect();
        publisher.publish(topic, new MqttMessage(payload.getBytes()));
        publisher.disconnect();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !kafkaConsumer.queueIsEmpty());
        String message = kafkaConsumer.getLastMessage();
        assertThat(message).isNotNull().contains("25.3");
    }
}

