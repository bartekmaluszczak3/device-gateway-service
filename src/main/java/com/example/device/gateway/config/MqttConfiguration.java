package com.example.device.gateway.config;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
@ConfigurationProperties("service.mqtt")
public class MqttConfiguration {

    private String brokerUrl;
    private String clientId;
    private String topicFilter;
}
