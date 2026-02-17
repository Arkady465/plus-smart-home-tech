package ru.yandex.practicum.handlers.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final SensorRepository sensorRepository;

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioAddedEventAvro scenarioAddedEvent = (ScenarioAddedEventAvro) event.getPayload();

        Optional<Scenario> scenarioOpt = scenarioRepository.findByHubIdAndName(event.getHubId(),
                scenarioAddedEvent.getName());

        Scenario scenario;
        if (scenarioOpt.isEmpty()) {
            // Новый сценарий
            scenario = scenarioRepository.save(mapToScenario(event));
            log.debug("Создан новый сценарий: hubId={}, name={}", event.getHubId(), scenarioAddedEvent.getName());
        } else {
            // Существующий сценарий — удаляем старые условия и действия перед обновлением
            scenario = scenarioOpt.get();
            log.debug("Обновление существующего сценария: id={}, hubId={}, name={}",
                    scenario.getId(), event.getHubId(), scenarioAddedEvent.getName());

            // Удаляем связанные условия и действия (предполагается, что связи настроены правильно)
            conditionRepository.deleteByScenario(scenario);
            actionRepository.deleteByScenario(scenario);
            // Очищаем коллекции в сущности (если нужно, но они не используются далее в этой транзакции)
            // После удаления можно сохранить новые условия и действия
        }

        // Сохраняем условия и действия только если все необходимые сенсоры существуют
        if (checkSensorsInScenarioConditions(scenarioAddedEvent, event.getHubId())) {
            Set<Condition> conditions = mapToCondition(scenarioAddedEvent, scenario);
            conditionRepository.saveAll(conditions);
            log.debug("Сохранено {} условий для сценария {}", conditions.size(), scenario.getName());
        } else {
            log.warn("Не все сенсоры для условий сценария {} существуют, условия не сохранены",
                    scenarioAddedEvent.getName());
        }

        if (checkSensorsInScenarioActions(scenarioAddedEvent, event.getHubId())) {
            Set<Action> actions = mapToAction(scenarioAddedEvent, scenario);
            actionRepository.saveAll(actions);
            log.debug("Сохранено {} действий для сценария {}", actions.size(), scenario.getName());
        } else {
            log.warn("Не все сенсоры для действий сценария {} существуют, действия не сохранены",
                    scenarioAddedEvent.getName());
        }
    }

    @Override
    public String getPayloadType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    private Scenario mapToScenario(HubEventAvro event) {
        ScenarioAddedEventAvro scenarioAddedEvent = (ScenarioAddedEventAvro) event.getPayload();

        return Scenario.builder()
                .name(scenarioAddedEvent.getName())
                .hubId(event.getHubId())
                .build();
    }

    private Set<Condition> mapToCondition(ScenarioAddedEventAvro scenarioAddedEvent, Scenario scenario) {
        // Собираем все ID сенсоров, используемых в условиях
        List<String> sensorIds = scenarioAddedEvent.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .distinct()
                .collect(Collectors.toList());

        // Загружаем сенсоры одним запросом
        Map<String, Sensor> sensorMap = sensorRepository.findAllById(sensorIds).stream()
                .collect(Collectors.toMap(Sensor::getId, sensor -> sensor));

        return scenarioAddedEvent.getConditions().stream()
                .map(c -> {
                    Sensor sensor = sensorMap.get(c.getSensorId());
                    if (sensor == null) {
                        // Эта ситуация не должна возникать, т.к. перед вызовом выполняется проверка,
                        // но на всякий случай логируем и пропускаем условие
                        log.error("Сенсор с ID {} не найден при создании условия для сценария {}",
                                c.getSensorId(), scenario.getName());
                        return null;
                    }
                    return Condition.builder()
                            .sensor(sensor)
                            .scenario(scenario)
                            .type(c.getType())
                            .operation(c.getOperation())
                            .value(convertConditionValue(c.getValue()))
                            .build();
                })
                .filter(condition -> condition != null)
                .collect(Collectors.toSet());
    }

    private Set<Action> mapToAction(ScenarioAddedEventAvro scenarioAddedEvent, Scenario scenario) {
        log.debug("Обрабатываем список действий для сценария {}: {}", scenario.getName(), scenarioAddedEvent.getActions());

        List<String> sensorIds = scenarioAddedEvent.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .distinct()
                .collect(Collectors.toList());

        Map<String, Sensor> sensorMap = sensorRepository.findAllById(sensorIds).stream()
                .collect(Collectors.toMap(Sensor::getId, sensor -> sensor));

        return scenarioAddedEvent.getActions().stream()
                .map(action -> {
                    Sensor sensor = sensorMap.get(action.getSensorId());
                    if (sensor == null) {
                        log.error("Сенсор с ID {} не найден при создании действия для сценария {}",
                                action.getSensorId(), scenario.getName());
                        return null;
                    }
                    return Action.builder()
                            .sensor(sensor)
                            .scenario(scenario)
                            .type(action.getType())
                            .value(action.getValue())
                            .build();
                })
                .filter(action -> action != null)
                .collect(Collectors.toSet());
    }

    private Integer convertConditionValue(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else {
            log.warn("Неизвестный тип значения условия: {}. Будет использовано значение по умолчанию 0", value);
            return 0;
        }
    }

    private boolean checkSensorsInScenarioConditions(ScenarioAddedEventAvro scenarioAddedEvent, String hubId) {
        List<String> sensorIds = scenarioAddedEvent.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .collect(Collectors.toList());
        return sensorRepository.existsByIdInAndHubId(sensorIds, hubId);
    }

    private boolean checkSensorsInScenarioActions(ScenarioAddedEventAvro scenarioAddedEvent, String hubId) {
        List<String> sensorIds = scenarioAddedEvent.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .collect(Collectors.toList());
        return sensorRepository.existsByIdInAndHubId(sensorIds, hubId);
    }
}