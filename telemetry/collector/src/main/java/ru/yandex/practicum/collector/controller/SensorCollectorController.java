package ru.yandex.practicum.collector.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.mapper.SensorEventAvroMapper;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.collector.serializer.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@RestController
@RequestMapping("/events/sensors")
@RequiredArgsConstructor
public class SensorCollectorController {

    private final KafkaProducer<String, byte[]> producer;

    @PostMapping
    public void collect(@RequestBody SensorEvent event) {
        SensorEventAvro avro = SensorEventAvroMapper.map(event);
        byte[] payload = AvroSerializer.serialize(avro);

        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(
                        "telemetry.sensors.v1",
                        null,
                        event.getTimestamp().toEpochMilli(),
                        event.getId(),
                        payload
                );

        producer.send(record);
    }
}
