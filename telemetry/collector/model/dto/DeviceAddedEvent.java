package ru.yandex.practicum.kafka.telemetry.collector.model.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.kafka.telemetry.collector.model.DeviceType;
import ru.yandex.practicum.kafka.telemetry.collector.model.HubEventType;

@Getter
@Setter
@ToString(callSuper = true)
public class DeviceAddedEvent extends HubEvent {
    private String id;
    private DeviceType type;

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED_EVENT;
    }
}