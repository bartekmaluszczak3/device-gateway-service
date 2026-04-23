package com.device.service.kafka.publisher;

import com.device.service.kafka.event.DataReceivedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventPublisher {
    private final KafkaTemplate<String, DataReceivedEvent> kafkaTemplate;
    private final String topic;

    public EventPublisher(KafkaTemplate<String, DataReceivedEvent> kafkaTemplate,
                          @Value("${kafka.topics.device-data}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(DataReceivedEvent event) {
        log.info("Sending Data Received Event");
        kafkaTemplate.send(topic, event.getDeviceId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Cannot send event for deviceId={}: {}",
                                event.getDeviceId(), ex.getMessage(), ex);
                    } else {
                        log.debug("Event sent deviceId={} partition={} offset={}",
                                event.getDeviceId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
