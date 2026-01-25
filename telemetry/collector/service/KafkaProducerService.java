package ru.yandex.practicum.kafka.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.collector.converter.AvroConverter;
import ru.yandex.practicum.kafka.telemetry.collector.model.dto.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.model.dto.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AvroConverter avroConverter;

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";

    public void sendSensorEvent(SensorEvent event) {
        try {
            SensorEventAvro avroEvent = avroConverter.convertToAvro(event);
            kafkaTemplate.send(SENSORS_TOPIC, event.getId(), avroEvent)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.debug("Sensor event sent successfully: id={}, hubId={}, type={}",
                                    event.getId(), event.getHubId(), event.getType());
                        } else {
                            log.error("Failed to send sensor event: {}", event, ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Error converting or sending sensor event: {}", event, e);
            throw new RuntimeException("Failed to process sensor event", e);
        }
    }

    public void sendHubEvent(HubEvent event) {
        try {
            HubEventAvro avroEvent = avroConverter.convertToAvro(event);
            kafkaTemplate.send(HUBS_TOPIC, event.getHubId(), avroEvent)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.debug("Hub event sent successfully: hubId={}, type={}",
                                    event.getHubId(), event.getType());
                        } else {
                            log.error("Failed to send hub event: {}", event, ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Error converting or sending hub event: {}", event, e);
            throw new RuntimeException("Failed to process hub event", e);
        }
    }
}