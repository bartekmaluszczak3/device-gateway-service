package com.device.service.kafka.event;

import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;
import java.util.Map;

@SuperBuilder
@Jacksonized
@Getter
public class DataReceivedEvent extends Event {
    private Instant occurredAt;
    private Map<String, Object> payload;
}