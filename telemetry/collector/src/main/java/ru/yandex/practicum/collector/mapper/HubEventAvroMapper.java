package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class HubEventAvroMapper {

    public static HubEventAvro map(HubEvent event) {

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapPayload(event))
                .build();
    }

    private static Object mapPayload(HubEvent event) {

        if (event instanceof DeviceAddedEvent e) {
            return DeviceAddedEventAvro.newBuilder()
                    .setId(e.getId())
                    .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
                    .build();
        }

        if (event instanceof DeviceRemovedEvent e) {
            return DeviceRemovedEventAvro.newBuilder()
                    .setId(e.getId())
                    .build();
        }

        if (event instanceof ScenarioAddedEvent e) {
            return ScenarioAddedEventAvro.newBuilder()
                    .setId(e.getScenarioId())
                    .setConditions(
                            e.getConditions().stream()
                                    .map(c -> ScenarioConditionAvro.newBuilder()
                                            .setSensorId(c.getSensorId())
                                            .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                            .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                                            .setValue(c.getValue())
                                            .build()
                                    ).toList()
                    )
                    .setActions(
                            e.getActions().stream()
                                    .map(a -> DeviceActionAvro.newBuilder()
                                            .setSensorId(a.getSensorId())
                                            .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                            .setValue(a.getValue())
                                            .build()
                                    ).toList()
                    )
                    .build();
        }

        if (event instanceof ScenarioRemovedEvent e) {
            return ScenarioRemovedEventAvro.newBuilder()
                    .setId(e.getScenarioId())
                    .build();
        }

        throw new IllegalArgumentException("Unsupported hub event: " + event.getClass());
    }
}
