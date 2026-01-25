package ru.yandex.practicum.collector.model.hub;

public class DeviceAction {

    private String sensorId;
    private String type;
    private Integer value;

    public String getSensorId() {
        return sensorId;
    }

    public String getType() {
        return type;
    }

    public Integer getValue() {
        return value;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setValue(Integer value) {
        this.value = value;
    }
}
