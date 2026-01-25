package ru.yandex.practicum.kafka.telemetry.collector.model.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.kafka.telemetry.collector.model.ActionType;

@Getter
@Setter
@ToString
public class DeviceAction {
    private String sensorId;
    private ActionType type;
    private Integer value;
}