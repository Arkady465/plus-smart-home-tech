package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class SensorEventAvroMapper {

    public SensorEventAvro map(SensorEvent event) {

        Object payload;

        if (event instanceof MotionSensorEvent e) {

            payload = MotionSensorPayloadAvro.newBuilder()
                    .setMotion(e.isMotion())
                    .build();

        } else if (event instanceof TemperatureSensorEvent e) {

            payload = ClimateSensorPayloadAvro.newBuilder()
                    .setTemperature(e.getValue())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2())
                    .build();

        } else if (event instanceof LightSensorEvent e) {

            payload = LightSensorPayloadAvro.newBuilder()
                    .setLuminosity(e.getLuminosity())
                    .build();

        } else if (event instanceof SwitchSensorEvent e) {

            payload = SwitchSensorPayloadAvro.newBuilder()
                    .setState(e.isState())
                    .build();

        } else {
            throw new IllegalArgumentException("Unsupported SensorEvent: " + event.getClass());
        }

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
