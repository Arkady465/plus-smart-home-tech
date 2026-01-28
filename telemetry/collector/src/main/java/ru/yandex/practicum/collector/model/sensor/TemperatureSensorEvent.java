package ru.yandex.practicum.collector.model.sensor;

public class TemperatureSensorEvent extends SensorEvent {

    private int temperatureC;
    private int temperatureF;

    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }

    public int getTemperatureC() {
        return temperatureC;
    }

    public int getTemperatureF() {
        return temperatureF;
    }

    public void setTemperatureC(int temperatureC) {
        this.temperatureC = temperatureC;
    }

    public void setTemperatureF(int temperatureF) {
        this.temperatureF = temperatureF;
    }

    public Integer getValue() {
        return getTemperatureC();
    }

    public Object getHumidity() {
        throw new UnsupportedOperationException("Temperature sensor does not measure humidity");
    }
}
