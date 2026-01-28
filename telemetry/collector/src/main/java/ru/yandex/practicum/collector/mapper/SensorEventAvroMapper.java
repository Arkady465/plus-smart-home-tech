package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class SensorEventAvroMapper {

    public SensorEventAvro map(SensorEvent event) {

        Object payload;

        if (event instanceof MotionSensorEvent e) {
            payload = MotionSensorEventAvro.newBuilder()
                    .setMotion(e.isMotion())
                    .build();

        } else if (event instanceof TemperatureSensorEvent e) {
            payload = TemperatureSensorEventAvro.newBuilder()
                    .setTemperature(e.getTemperature())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build();

        } else if (event instanceof LightSensorEvent e) {
            payload = LightSensorEventAvro.newBuilder()
                    .setLuminosity(e.getLuminosity())
                    .build();

        } else if (event instanceof SwitchSensorEvent e) {
            payload = SwitchSensorEventAvro.newBuilder()
                    .setState(e.isState())
                    .build();

        } else {
            throw new IllegalArgumentException("Unsupported sensor event: " + event.getClass());
        }

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setSensorId(event.getSensorId())
                .setTimestamp(event.getTimestamp()) // Instant
                .setPayload(payload)
                .build();
    }
}
