package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class SnapshotStorage {
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        // Используем строковое представление hubId как ключ для согласованности
        String hubIdKey = event.getHubId().toString();
        SensorsSnapshotAvro snapshot = snapshots.get(hubIdKey);
        if (snapshot == null) {
            snapshot = new SensorsSnapshotAvro();
            snapshot.setHubId(event.getHubId());
            snapshot.setTimestamp(event.getTimestamp());
            snapshot.setSensorsState(new HashMap<>());
        }

        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());
        if (oldState != null) {
            // Если пришло более старое событие или данные не изменились, снапшот не обновляем
            if (oldState.getTimestamp().isAfter(event.getTimestamp()) ||
                    oldState.getData().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        SensorStateAvro newState = new SensorStateAvro();
        newState.setTimestamp(event.getTimestamp());
        newState.setData(event.getPayload());
        snapshot.getSensorsState().put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());
        snapshots.put(hubIdKey, snapshot);
        return Optional.of(snapshot);
    }
}