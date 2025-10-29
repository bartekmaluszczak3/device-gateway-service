package com.example.device.gateway.service;

import com.example.device.gateway.config.MqttConfiguration;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MqttTelemetryListener {

    private final MqttConfiguration mqttConfiguration;
    private final MqttCallBackExtend mqttCallBackExtend;
    public MqttTelemetryListener(KafkaTemplate<String, String> kafkaTemplate, MqttConfiguration mqttConfiguration){
        this.mqttCallBackExtend = new MqttCallBackExtend(kafkaTemplate);
        this.mqttConfiguration = mqttConfiguration;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startListener() throws MqttException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        MqttClient client = new MqttClient(mqttConfiguration.getBrokerUrl(), mqttConfiguration.getClientId(), new MemoryPersistence());
        client.setCallback(mqttCallBackExtend);
        client.connect(connOpts);
        client.subscribe(mqttConfiguration.getTopicFilter());
    }
}
