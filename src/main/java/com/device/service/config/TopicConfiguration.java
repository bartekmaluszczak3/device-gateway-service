package com.device.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "kafka")
@Configuration
public class TopicConfiguration {
    private Map<String, String> topics = new HashMap<>();

    public Map<String, String> getTopics(){
        return topics;
    }

    public String getTopicForEventType(String eventType){
        String topic = topics.get(eventType);
        if(topic == null){
            throw new IllegalArgumentException("No topic configured for " + eventType);
        }
        return topic;
    }
}
