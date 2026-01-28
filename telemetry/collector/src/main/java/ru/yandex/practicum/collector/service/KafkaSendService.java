package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
public class KafkaSendService {

    private final KafkaProducer<String, Object> kafkaProducer;

    @Value("${kafka.topic.hubs}")
    private String hubTopic;

    @Value("${kafka.topic.sensors}")
    private String sensorTopic;

    public void sendHubEvent(HubEventAvro event) {
        kafkaProducer.send(
                new ProducerRecord<>(
                        hubTopic,
                        null,
                        event.getTimestamp().toEpochMilli(),
                        event.getHubId(),
                        event
                )
        );
    }

    public void sendSensorEvent(SensorEventAvro event) {
        kafkaProducer.send(
                new ProducerRecord<>(
                        sensorTopic,
                        null,
                        event.getTimestamp().toEpochMilli(),
                        event.getHubId(),
                        event
                )
        );
    }
}
