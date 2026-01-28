package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class SensorEventAvroMapper {

    public SensorEventAvro map(SensorEvent event) {

        Object payload;

        if (event instanceof MotionSensorEvent) {
            MotionSensorEvent e = (MotionSensorEvent) event;

            payload = MotionSensorPayloadAvro.newBuilder()
                    .setMotion(e.isMotion())
                    .build();

        } else if (event instanceof ClimateSensorEvent) {
            ClimateSensorEvent e = (ClimateSensorEvent) event;

            payload = ClimateSensorPayloadAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build();

        } else if (event instanceof LightSensorEvent) {
            LightSensorEvent e = (LightSensorEvent) event;

            payload = LightSensorPayloadAvro.newBuilder()
                    .setLuminosity(e.getLuminosity())
                    .build();

        } else if (event instanceof SwitchSensorEvent) {
            SwitchSensorEvent e = (SwitchSensorEvent) event;

            payload = SwitchSensorPayloadAvro.newBuilder()
                    .setState(e.isState())
                    .build();

        } else if (event instanceof TemperatureSensorEvent) {
            TemperatureSensorEvent e = (TemperatureSensorEvent) event;
            // map temperature-only sensor into climate payload with defaults for missing fields
            payload = ClimateSensorPayloadAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(0)
                    .setCo2Level(0)
                    .build();

        } else {
            throw new IllegalArgumentException("Unsupported SensorEvent: " + event.getClass());
        }

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(payload)
                .build();
    }
}