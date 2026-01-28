package ru.yandex.practicum.collector.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.mapper.HubEventAvroMapper;
import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.collector.serializer.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@RestController
@RequestMapping("/events/hubs")
@RequiredArgsConstructor
public class HubCollectorController {

    private final KafkaProducer<String, byte[]> producer;

    @PostMapping
    public void collect(@RequestBody HubEvent event) {
        HubEventAvro avro = HubEventAvroMapper.map(event);
        byte[] payload = AvroSerializer.serialize(avro);

        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(
                        "telemetry.hubs.v1",
                        null,
                        event.getTimestamp().toEpochMilli(),
                        event.getHubId(),
                        payload
                );

        producer.send(record);
    }
}
