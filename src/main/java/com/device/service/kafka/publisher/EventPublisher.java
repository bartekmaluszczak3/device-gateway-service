package com.device.service.kafka.publisher;

import com.device.service.config.TopicConfiguration;
import com.device.service.kafka.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventPublisher {
    private final KafkaTemplate<String, Event> kafkaTemplate;
    public EventPublisher(KafkaTemplate<String, Event> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(Event event, String topic) {
        log.info("Sending Data Received Event");
        kafkaTemplate.send(topic, event.getDeviceId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Cannot send event for deviceId={}: {}",
                                event.getDeviceId(), ex.getMessage(), ex);
                    } else {
                        log.debug("Event sent deviceId={} partition={} offset={}", event.getDeviceId(), result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
