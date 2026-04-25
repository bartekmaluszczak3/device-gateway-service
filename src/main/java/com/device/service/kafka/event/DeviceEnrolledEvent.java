package com.device.service.kafka.event;

import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@SuperBuilder
@Jacksonized
@Getter
public class DeviceEnrolledEvent extends Event { }
