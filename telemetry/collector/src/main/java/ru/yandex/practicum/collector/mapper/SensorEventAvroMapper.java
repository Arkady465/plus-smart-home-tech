package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class SensorEventAvroMapper {

    public static SensorEventAvro map(SensorEvent event) {

        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapPayload(event))
                .build();
    }

    private static Object mapPayload(SensorEvent event) {

        if (event instanceof LightSensorEvent e) {
            return LightSensorAvro.newBuilder()
                    .setLuminosity(e.getLuminosity())
                    .build();
        }

        if (event instanceof MotionSensorEvent e) {
            return MotionSensorAvro.newBuilder()
                    .setMotion(e.isMotion())
                    .build();
        }

        if (event instanceof SwitchSensorEvent e) {
            return SwitchSensorAvro.newBuilder()
                    .setState(e.isState())
                    .build();
        }

        if (event instanceof ClimateSensorEvent e) {
            return ClimateSensorAvro.newBuilder()
                    .setTemperature(e.getTemperature())
                    .setHumidity(e.getHumidity())
                    .setCo2(e.getCo2())
                    .build();
        }

        throw new IllegalArgumentException("Unsupported sensor event: " + event.getClass());
    }
}
