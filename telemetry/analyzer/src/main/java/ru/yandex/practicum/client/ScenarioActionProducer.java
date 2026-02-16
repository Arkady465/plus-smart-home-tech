package ru.yandex.practicum.client;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.model.Action;

import java.time.Instant;

@Slf4j
@Service
public class ScenarioActionProducer {
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterStub;

    public ScenarioActionProducer(
            @GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterStub) {
        this.hubRouterStub = hubRouterStub;
    }

    public void sendAction(Action action) {
        DeviceActionRequest actionRequest = mapToActionRequest(action);
        log.debug("Сформирован запрос на выполнение действия: {}", actionRequest);

        try {
            Empty response = hubRouterStub.handleDeviceAction(actionRequest);
            log.info("Действие для сценария '{}' (hubId={}) отправлено: сенсор={}, тип={}, значение={}",
                    action.getScenario().getName(),
                    action.getScenario().getHubId(),
                    action.getSensor().getId(),
                    action.getType(),
                    action.getValue());
            if (response.isInitialized()) {
                log.debug("Получен ответ от hub-router: запрос успешно обработан");
            } else {
                log.debug("Ответ от hub-router не содержит данных (возможно, пустой)");
            }
        } catch (RuntimeException e) {
            log.error("Ошибка при отправке действия в hub-router: сценарий='{}', hubId={}, сенсор={}, тип={}, значение={}",
                    action.getScenario().getName(),
                    action.getScenario().getHubId(),
                    action.getSensor().getId(),
                    action.getType(),
                    action.getValue(),
                    e);
        }
    }

    private DeviceActionRequest mapToActionRequest(Action action) {
        log.debug("Преобразование действия в DeviceActionRequest: {}", action);
        return DeviceActionRequest.newBuilder()
                .setHubId(action.getScenario().getHubId())
                .setScenarioName(action.getScenario().getName())
                .setAction(DeviceActionProto.newBuilder()
                        .setSensorId(action.getSensor().getId())
                        .setType(mapActionType(action.getType()))
                        .setValue(action.getValue())
                        .build())
                .setTimestamp(setTimestamp())
                .build();
    }

    private ActionTypeProto mapActionType(ActionTypeAvro actionType) {
        log.debug("Маппинг типа действия: {}", actionType);
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }

    private Timestamp setTimestamp() {
        Instant instant = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}