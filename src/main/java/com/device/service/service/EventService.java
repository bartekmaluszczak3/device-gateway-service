package com.device.service.service;

import com.device.service.config.TopicConfiguration;
import com.device.service.kafka.event.DataReceivedEvent;
import com.device.service.kafka.event.DeviceEnrolledEvent;
import com.device.service.kafka.event.EventType;
import com.device.service.kafka.publisher.EventPublisher;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
@AllArgsConstructor
@Slf4j
public class EventService {
    private final EventPublisher eventPublisher;
    private final TopicConfiguration topicConfiguration;

    public void sendDataReceivedEvent(X509Certificate x509Certificate, Map<String, Object> payload){
        String deviceId = extractDeviceId(x509Certificate);
        DataReceivedEvent dataReceivedEvent = DataReceivedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .occurredAt(Instant.now())
                .deviceId(deviceId)
                .eventType(EventType.DATA_RECEIVED_EVENT)
                .payload(payload)
                .build();
        eventPublisher.publish(dataReceivedEvent, topicConfiguration.getTopicForEventType(EventType.DATA_RECEIVED_EVENT.toString()));
    }

    public void sendDeviceEnrolledEvent(X509Certificate x509Certificate){
        String deviceId = extractDeviceId(x509Certificate);
        DeviceEnrolledEvent deviceEnrolledEvent = DeviceEnrolledEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .occurredAt(Instant.now())
                .deviceId(deviceId)
                .eventType(EventType.DEVICE_ENROLLED_EVENT)
                .build();
        eventPublisher.publish(deviceEnrolledEvent, topicConfiguration.getTopicForEventType(EventType.DEVICE_ENROLLED_EVENT.toString()));
    }

    private String extractDeviceId(X509Certificate cert) {
        for (String part : cert.getSubjectX500Principal().getName().split(",")) {
            part = part.trim();
            if (part.startsWith("CN=")) return part.substring(3);
        }
        throw new IllegalArgumentException(
                "Certificate does not contain CN: " + cert.getSubjectX500Principal().getName());
    }
}
