package ru.yandex.practicum.collector.model.hub;

public class DeviceAddedEvent extends HubEvent {

    private String id;
    private String deviceType;

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }

    public String getId() {
        return id;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }
}
