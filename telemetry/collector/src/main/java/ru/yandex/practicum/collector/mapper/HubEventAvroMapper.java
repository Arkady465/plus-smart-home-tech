package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.stream.Collectors;

@Component
public class HubEventAvroMapper {

    public HubEventAvro map(HubEvent event) {

        Object payload;

        if (event instanceof DeviceAddedEvent e) {
            payload = DeviceAddedEventAvro.newBuilder()
                    .setId(e.getDeviceId())
                    .setType(DeviceTypeAvro.valueOf(e.getDeviceType()))
                    .build();

        } else if (event instanceof DeviceRemovedEvent e) {
            payload = DeviceRemovedEventAvro.newBuilder()
                    .setId(e.getDeviceId())
                    .build();

        } else if (event instanceof ScenarioAddedEvent e) {
            payload = ScenarioAddedEventAvro.newBuilder()
                    .setName(e.getName())
                    .setConditions(
                            e.getConditions().stream()
                                    .map(c -> ScenarioConditionAvro.newBuilder()
                                            .setSensorId(c.getSensorId())
                                            .setType(ConditionTypeAvro.valueOf(c.getType()))
                                            .setOperation(ConditionOperationAvro.valueOf(c.getOperation()))
                                            .setValue(c.getValue())
                                            .build())
                                    .collect(Collectors.toList())
                    )
                    .setActions(
                            e.getActions().stream()
                                    .map(a -> DeviceActionAvro.newBuilder()
                                            .setSensorId(a.getSensorId())
                                            .setType(ActionTypeAvro.valueOf(a.getType()))
                                            .setValue(a.getValue())
                                            .build())
                                    .collect(Collectors.toList())
                    )
                    .build();

        } else if (event instanceof ScenarioRemovedEvent e) {
            payload = ScenarioRemovedEventAvro.newBuilder()
                    .setName(e.getName())
                    .build();

        } else {
            throw new IllegalArgumentException("Unsupported hub event: " + event.getClass());
        }

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(payload)
                .build();
    }
}
