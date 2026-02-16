package ru.yandex.practicum.handlers.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.client.ScenarioActionProducer;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class SnapshotHandler {
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;
    private final ActionRepository actionRepository;
    private final ScenarioActionProducer scenarioActionProducer;

    public void handleSnapshot(SensorsSnapshotAvro sensorsSnapshot) {
        log.debug("Обработка снапшота для hubId={}", sensorsSnapshot.getHubId());
        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshot.getSensorsState();
        List<Scenario> scenarios = scenarioRepository.findByHubId(sensorsSnapshot.getHubId());
        scenarios.stream()
                .filter(scenario -> handleScenario(scenario, sensorStateMap))
                .forEach(scenario -> {
                    log.info("Выполнение сценария: hubId={}, имя сценария={}",
                            sensorsSnapshot.getHubId(), scenario.getName());
                    sendScenarioActions(scenario);
                });
    }

    private boolean handleScenario(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<Condition> conditions = conditionRepository.findAllByScenario(scenario);
        log.debug("Проверка условий сценария '{}': условий {}", scenario.getName(), conditions.size());

        // Условия считаются выполненными, если ни одно условие не ложно
        return conditions.stream().noneMatch(condition -> !checkCondition(condition, sensorStateMap));
    }

    private boolean checkCondition(Condition condition, Map<String, SensorStateAvro> sensorStateMap) {
        String sensorId = condition.getSensor().getId();
        SensorStateAvro sensorState = sensorStateMap.get(sensorId);
        if (sensorState == null) {
            log.debug("Сенсор {} не найден в снапшоте, условие не выполнено", sensorId);
            return false;
        }
        switch (condition.getType()) {
            case LUMINOSITY:
                LightSensorAvro lightSensor = (LightSensorAvro) sensorState.getData();
                return handleOperation(condition, lightSensor.getLuminosity());
            case TEMPERATURE:
                ClimateSensorAvro temperatureSensor = (ClimateSensorAvro) sensorState.getData();
                return handleOperation(condition, temperatureSensor.getTemperatureC());
            case MOTION:
                MotionSensorAvro motionSensor = (MotionSensorAvro) sensorState.getData();
                return handleOperation(condition, motionSensor.getMotion() ? 1 : 0);
            case SWITCH:
                SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorState.getData();
                return handleOperation(condition, switchSensor.getState() ? 1 : 0);
            case CO2LEVEL:
                ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                return handleOperation(condition, climateSensor.getCo2Level());
            case HUMIDITY:
                ClimateSensorAvro humiditySensor = (ClimateSensorAvro) sensorState.getData();
                return handleOperation(condition, humiditySensor.getHumidity());
            default:
                log.error("Неизвестный тип условия: {}", condition.getType());
                return false;
        }
    }

    private boolean handleOperation(Condition condition, Integer currentValue) {
        ConditionOperationAvro operation = condition.getOperation();
        Integer targetValue = condition.getValue();

        if (operation == null) {
            log.error("Операция условия не задана для условия: {}", condition);
            return false;
        }

        switch (operation) {
            case EQUALS:
                return Objects.equals(targetValue, currentValue);
            case LOWER_THAN:
                return currentValue < targetValue;
            case GREATER_THAN:
                return currentValue > targetValue;
            default:
                log.error("Неподдерживаемая операция: {}", operation);
                return false;
        }
    }

    private void sendScenarioActions(Scenario scenario) {
        log.debug("Отправка действий для сценария '{}'", scenario.getName());
        actionRepository.findAllByScenario(scenario).forEach(scenarioActionProducer::sendAction);
    }
}