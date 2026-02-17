package ru.yandex.practicum.processors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handlers.event.HubEventHandler;
import ru.yandex.practicum.handlers.event.HubEventHandlers;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    private final KafkaConsumer<String, HubEventAvro> hubConsumer;
    private final HubEventHandlers handlers;
    @Value("${kafka.topics.hubs}")
    private String hubsTopic;

    @Override
    public void run() {
        try {
            hubConsumer.subscribe(List.of(hubsTopic));
            log.info("Подписались на топик хабов: {}", hubsTopic);

            Runtime.getRuntime().addShutdownHook(new Thread(hubConsumer::wakeup));
            log.debug("Добавлен shutdown hook с wakeup");

            Map<String, HubEventHandler> handlerMap = handlers.getHandlers();

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = hubConsumer.poll(Duration.ofMillis(1000));
                boolean hasErrors = false;

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    try {
                        HubEventAvro event = record.value();
                        String payloadName = event.getPayload().getClass().getSimpleName();
                        log.debug("Получено событие хаба типа: {}, ключ={}", payloadName, record.key());

                        HubEventHandler handler = handlerMap.get(payloadName);
                        if (handler != null) {
                            handler.handle(event);
                            log.debug("Событие {} успешно обработано", payloadName);
                        } else {
                            log.error("Не найден обработчик для события типа {}", payloadName);
                            throw new IllegalArgumentException("Не могу найти обработчик для события " + payloadName);
                        }
                    } catch (Exception e) {
                        log.error("Ошибка при обработке записи хаба: ключ={}, значение={}",
                                record.key(), record.value(), e);
                        hasErrors = true;
                    }
                }

                // Коммитим оффсеты только если не было ошибок и есть обработанные записи
                if (!hasErrors && !records.isEmpty()) {
                    try {
                        hubConsumer.commitSync();
                        log.debug("Оффсеты событий хабов зафиксированы");
                    } catch (Exception e) {
                        log.error("Ошибка при фиксации оффсетов событий хабов", e);

                    }
                } else if (hasErrors) {
                    log.warn("В текущей пачке событий хабов были ошибки, коммит оффсетов пропущен");
                }
            }
        } catch (WakeupException ignored) {
            log.info("Получен WakeupException, инициируем завершение работы процессора событий хабов");
        } catch (Exception e) {
            log.error("Необработанная ошибка в процессоре событий хабов", e);
        } finally {
            // Закрываем consumer без дополнительного коммита, чтобы не зафиксировать ошибочные оффсеты
            log.info("Завершение работы процессора событий хабов, закрываем consumer");
            try {
                hubConsumer.close();
            } catch (Exception e) {
                log.error("Ошибка при закрытии consumer событий хабов", e);
            }
        }
    }
}