package ru.yandex.practicum.collector.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.mapper.HubEventAvroMapper;
import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.collector.service.KafkaSendService;

@RestController
@RequestMapping("/events/hubs")
@RequiredArgsConstructor
public class HubCollectorController {

    private final HubEventAvroMapper mapper;
    private final KafkaSendService kafkaSendService;

    @PostMapping
    public void collect(@RequestBody HubEvent event) {
        kafkaSendService.sendHubEvent(mapper.map(event));
    }
}
