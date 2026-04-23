package com.device.service.utils;

import com.device.service.kafka.event.DataReceivedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class KafkaTestConsumer {
    private final String bootstrapServers;
    private final ObjectMapper objectMapper;

    public KafkaTestConsumer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
    }

    public <T> List<T> consumeEvents(String topic, Duration timeout,
                                     Class<T> eventType) throws Exception {
        List<T> result = new ArrayList<>();
        for (ConsumerRecord<String, String> record : consumeRawRecords(topic, timeout)) {
            result.add(objectMapper.readValue(record.value(), eventType));
        }
        return result;
    }

    public List<ConsumerRecord<String, String>> consumeRawRecords(String topic, Duration timeout) {
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(StringDeserializer.class.getClassLoader());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildProps())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(300)).forEach(collected::add);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }

        return collected;
    }
    private Properties buildProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,          "test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,  9999);
        return props;
    }
}
