package ru.yandex.practicum.collector.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.mapper.SensorEventAvroMapper;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.collector.service.KafkaSendService;

@RestController
@RequestMapping("/events/sensors")
@RequiredArgsConstructor
public class SensorCollectorController {

    private final SensorEventAvroMapper mapper;
    private final KafkaSendService kafkaSendService;

    @PostMapping
    public void collect(@RequestBody SensorEvent event) {
        kafkaSendService.sendSensorEvent(mapper.map(event));
    }
}

