package ru.yandex.practicum.kafka.telemetry.collector.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.kafka.telemetry.collector.model.dto.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.model.dto.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.collector.service.KafkaProducerService;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
public class CollectorController {

    private final KafkaProducerService kafkaProducerService;

    public CollectorController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvent(
            @Valid @RequestBody SensorEvent event) {

        log.info("Received sensor event: id={}, hubId={}, type={}",
                event.getId(), event.getHubId(), event.getType());

        kafkaProducerService.sendSensorEvent(event);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/hub")
    public ResponseEntity<Void> collectHubEvent(
            @Valid @RequestBody HubEvent event) {

        log.info("Received hub event: hubId={}, type={}",
                event.getHubId(), event.getType());

        kafkaProducerService.sendHubEvent(event);
        return ResponseEntity.accepted().build();
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleException(Exception e) {
        log.error("Error processing request", e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Internal server error: " + e.getMessage());
    }
}