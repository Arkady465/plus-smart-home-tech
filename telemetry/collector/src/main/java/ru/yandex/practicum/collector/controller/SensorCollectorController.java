package ru.yandex.practicum.collector.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.mapper.SensorEventAvroMapper;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;

@RestController
@RequestMapping("/collect/sensors")
public class SensorCollectorController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public SensorCollectorController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public void collect(@RequestBody SensorEvent event) {
        kafkaTemplate.send(
                "telemetry.sensors.v1",
                event.getHubId(),
                SensorEventAvroMapper.map(event)
        );
    }
}
