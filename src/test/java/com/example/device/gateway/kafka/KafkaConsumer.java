package com.example.device.gateway.kafka;

import com.example.device.gateway.constants.TopicConstants;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class KafkaConsumer {
    BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

    @KafkaListener(topics = TopicConstants.TELEMETRY_TOPIC, groupId = "test-group")
    public void consume(String message) {
        receivedMessages.add(message);
    }

    public String getLastMessage() throws InterruptedException {
        return receivedMessages.take();
    }

    public boolean queueIsEmpty(){
       return receivedMessages.isEmpty();
    }
}
