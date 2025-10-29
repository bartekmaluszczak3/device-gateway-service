package com.example.device.gateway.service;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class MqttCallBackExtend implements MqttCallbackExtended {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public MqttCallBackExtend(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Override
    public void connectComplete(boolean b, String serverUrl) {
        log.info("Connected to MQTT broker {} ", serverUrl);
    }

    @Override
    public void connectionLost(Throwable throwable) {
        log.error("Connection lost ", throwable.getCause());
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        log.info("Received telemetry from topic " + topic);
        String payload = new String(mqttMessage.getPayload());
        log.debug("Payload {}", payload);
        String[] parts = topic.split("/");
        String deviceId = parts[2];
        kafkaTemplate.send("telemetry.raw", deviceId, payload);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
