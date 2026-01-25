package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class SensorEventAvroMapper {

    public static SensorEventAvro map(SensorEvent event) {
        Object payload;

        if (event instanceof LightSensorEvent e) {
            payload = LightSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setLuminosity(e.getLuminosity())
                    .build();

        } else if (event instanceof MotionSensorEvent e) {
            payload = MotionSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setMotion(e.isMotion())
                    .setVoltage(e.getVoltage())
                    .build();

        } else if (event instanceof SwitchSensorEvent e) {
            payload = SwitchSensorAvro.newBuilder()
                    .setState(e.isState())
                    .build();

        } else if (event instanceof ClimateSensorEvent e) {
            payload = ClimateSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build();

        } else if (event instanceof TemperatureSensorEvent e) {
            payload = TemperatureSensorAvro.newBuilder()
                    .setId(e.getId())
                    .setHubId(e.getHubId())
                    .setTimestamp(e.getTimestamp()) // ✅ Instant
                    .setTemperatureC(e.getTemperatureC())
                    .setTemperatureF(e.getTemperatureF())
                    .build();

        } else {
            throw new IllegalArgumentException("Unsupported sensor event: " + event.getClass());
        }

        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp()) // ✅ Instant
                .setPayload(payload)
                .build();
    }
}
