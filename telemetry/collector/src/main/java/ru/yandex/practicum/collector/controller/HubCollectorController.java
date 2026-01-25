package ru.yandex.practicum.collector.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.mapper.HubEventAvroMapper;
import ru.yandex.practicum.collector.model.hub.HubEvent;

@RestController
@RequestMapping("/collect/hubs")
public class HubCollectorController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public HubCollectorController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public void collect(@RequestBody HubEvent event) {
        kafkaTemplate.send(
                "telemetry.hubs.v1",
                event.getHubId(),
                HubEventAvroMapper.map(event)
        );
    }
}
