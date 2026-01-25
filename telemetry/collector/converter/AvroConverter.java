package ru.yandex.practicum.kafka.telemetry.collector.converter;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.collector.model.dto.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class AvroConverter {

    // Конвертация событий датчиков
    public SensorEventAvro convertToAvro(SensorEvent event) {
        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .build();

        if (event instanceof LightSensorEvent) {
            LightSensorEvent lightEvent = (LightSensorEvent) event;
            LightSensorAvro lightAvro = LightSensorAvro.newBuilder()
                    .setLinkQuality(lightEvent.getLinkQuality())
                    .setLuminosity(lightEvent.getLuminosity())
                    .build();
            avro.setPayload(lightAvro);

        } else if (event instanceof MotionSensorEvent) {
            MotionSensorEvent motionEvent = (MotionSensorEvent) event;
            MotionSensorAvro motionAvro = MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionEvent.getLinkQuality())
                    .setMotion(motionEvent.isMotion())
                    .setVoltage(motionEvent.getVoltage())
                    .build();
            avro.setPayload(motionAvro);

        } else if (event instanceof ClimateSensorEvent) {
            ClimateSensorEvent climateEvent = (ClimateSensorEvent) event;
            ClimateSensorAvro climateAvro = ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climateEvent.getTemperatureC())
                    .setHumidity(climateEvent.getHumidity())
                    .setCo2Level(climateEvent.getCo2Level())
                    .build();
            avro.setPayload(climateAvro);

        } else if (event instanceof SwitchSensorEvent) {
            SwitchSensorEvent switchEvent = (SwitchSensorEvent) event;
            SwitchSensorAvro switchAvro = SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.isState())
                    .build();
            avro.setPayload(switchAvro);

        } else if (event instanceof TemperatureSensorEvent) {
            TemperatureSensorEvent tempEvent = (TemperatureSensorEvent) event;
            TemperatureSensorAvro tempAvro = TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(tempEvent.getTemperatureC())
                    .setTemperatureF(tempEvent.getTemperatureF())
                    .build();
            avro.setPayload(tempAvro);
        }

        return avro;
    }

    // Конвертация событий хаба
    public HubEventAvro convertToAvro(HubEvent event) {
        HubEventAvro avro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .build();

        if (event instanceof DeviceAddedEvent) {
            DeviceAddedEvent deviceAdded = (DeviceAddedEvent) event;
            DeviceAddedEventAvro deviceAvro = DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAdded.getId())
                    .setType(convertDeviceType(deviceAdded.getType()))
                    .build();
            avro.setPayload(deviceAvro);

        } else if (event instanceof DeviceRemovedEvent) {
            DeviceRemovedEvent deviceRemoved = (DeviceRemovedEvent) event;
            DeviceRemovedEventAvro deviceAvro = DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemoved.getId())
                    .build();
            avro.setPayload(deviceAvro);

        } else if (event instanceof ScenarioAddedEvent) {
            ScenarioAddedEvent scenarioAdded = (ScenarioAddedEvent) event;
            ScenarioAddedEventAvro scenarioAvro = ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAdded.getName())
                    .setConditions(convertConditions(scenarioAdded.getConditions()))
                    .setActions(convertActions(scenarioAdded.getActions()))
                    .build();
            avro.setPayload(scenarioAvro);

        } else if (event instanceof ScenarioRemovedEvent) {
            ScenarioRemovedEvent scenarioRemoved = (ScenarioRemovedEvent) event;
            ScenarioRemovedEventAvro scenarioAvro = ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioRemoved.getName())
                    .build();
            avro.setPayload(scenarioAvro);
        }

        return avro;
    }

    private DeviceTypeAvro convertDeviceType(ru.yandex.practicum.kafka.telemetry.collector.model.DeviceType type) {
        return DeviceTypeAvro.valueOf(type.name());
    }

    private List<ScenarioConditionAvro> convertConditions(List<ScenarioCondition> conditions) {
        if (conditions == null) return List.of();

        return conditions.stream()
                .map(this::convertCondition)
                .collect(Collectors.toList());
    }

    private ScenarioConditionAvro convertCondition(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

        // Обработка значения условия
        if (condition.getValue() == null) {
            builder.setValue(null);
        } else if (condition.getValue() instanceof Integer) {
            builder.setValue((Integer) condition.getValue());
        } else if (condition.getValue() instanceof Boolean) {
            builder.setValue((Boolean) condition.getValue());
        }

        return builder.build();
    }

    private List<DeviceActionAvro> convertActions(List<DeviceAction> actions) {
        if (actions == null) return List.of();

        return actions.stream()
                .map(this::convertAction)
                .collect(Collectors.toList());
    }

    private DeviceActionAvro convertAction(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }
}