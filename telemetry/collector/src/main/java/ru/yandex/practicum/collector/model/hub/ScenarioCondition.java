package ru.yandex.practicum.collector.model.hub;

public class ScenarioCondition {

    private String sensorId;
    private String type;
    private String operation;
    private Object value;

    public String getSensorId() {
        return sensorId;
    }

    public String getType() {
        return type;
    }

    public String getOperation() {
        return operation;
    }

    public Object getValue() {
        return value;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
