package com.device.service.service;

import com.device.service.kafka.event.DataReceivedEvent;
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

    public void sendDataReceivedEvent(X509Certificate x509Certificate, Map<String, Object> payload){
        String deviceId = extractDeviceId(x509Certificate);
        DataReceivedEvent dataReceivedEvent = DataReceivedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .occurredAt(Instant.now())
                .deviceId(deviceId)
                .payload(payload)
                .build();
        eventPublisher.publish(dataReceivedEvent);
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
