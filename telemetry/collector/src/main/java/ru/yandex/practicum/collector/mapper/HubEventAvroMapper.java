package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class HubEventAvroMapper {

    public HubEventAvro map(HubEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("HubEvent must not be null");
        }

        Object payload;

        if (event instanceof DeviceAddedEvent) {
            DeviceAddedEvent e = (DeviceAddedEvent) event;
            payload = DeviceAddedEventAvro.newBuilder()
                    .setId(e.getId())
                    .setType(DeviceTypeAvro.valueOf(e.getDeviceType()))
                    .build();

        } else if (event instanceof DeviceRemovedEvent) {
            DeviceRemovedEvent e = (DeviceRemovedEvent) event;
            payload = DeviceRemovedEventAvro.newBuilder()
                    .setId(e.getId())
                    .build();

        } else if (event instanceof ScenarioAddedEvent) {
            ScenarioAddedEvent e = (ScenarioAddedEvent) event;
            List<ScenarioConditionAvro> conditions = e.getConditions().stream()
                    .map(c -> ScenarioConditionAvro.newBuilder()
                            .setSensorId(c.getSensorId())
                            .setType(ConditionTypeAvro.valueOf(c.getType()))
                            .setOperation(ConditionOperationAvro.valueOf(c.getOperation()))
                            // Avro generated types often expect primitive fields; convert to string if needed
                            .setValue(c.getValue() == null ? null : c.getValue().toString())
                            .build())
                    .collect(Collectors.toList());

            List<DeviceActionAvro> actions = e.getActions().stream()
                    .map(a -> DeviceActionAvro.newBuilder()
                            .setSensorId(a.getSensorId())
                            .setType(ActionTypeAvro.valueOf(a.getType()))
                            .setValue(a.getValue() == null ? null : a.getValue())
                            .build())
                    .collect(Collectors.toList());

            payload = ScenarioAddedEventAvro.newBuilder()
                    .setName(e.getName())
                    .setConditions(conditions)
                    .setActions(actions)
                    .build();

        } else if (event instanceof ScenarioRemovedEvent) {
            ScenarioRemovedEvent e = (ScenarioRemovedEvent) event;
            payload = ScenarioRemovedEventAvro.newBuilder()
                    .setName(e.getName())
                    .build();

        } else {
            throw new IllegalArgumentException("Unsupported HubEvent: " + event.getClass().getSimpleName());
        }

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(payload)
                .build();
    }
}