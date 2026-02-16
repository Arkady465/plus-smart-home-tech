package ru.yandex.practicum.processors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handlers.snapshot.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {
    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final SnapshotHandler snapshotHandler;
    @Value("${kafka.topics.snapshots}")
    private String snapshotsTopic;

    public void start() {
        try {
            snapshotConsumer.subscribe(List.of(snapshotsTopic));
            log.info("Подписались на топик снапшотов: {}", snapshotsTopic);

            Runtime.getRuntime().addShutdownHook(new Thread(snapshotConsumer::wakeup));
            log.debug("Добавили shutdown hook с wakeup");

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        snapshotConsumer.poll(Duration.ofMillis(1000));

                boolean hasErrors = false;

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        SensorsSnapshotAvro sensorsSnapshot = record.value();
                        log.debug("Получен снапшот для hubId={}, timestamp={}",
                                sensorsSnapshot.getHubId(), sensorsSnapshot.getTimestamp());

                        snapshotHandler.handleSnapshot(sensorsSnapshot);
                        log.debug("Снапшот обработан успешно");
                    } catch (Exception e) {
                        log.error("Ошибка при обработке снапшота: ключ={}, значение={}",
                                record.key(), record.value(), e);
                        hasErrors = true;
                    }
                }

                if (!hasErrors && !records.isEmpty()) {
                    try {
                        snapshotConsumer.commitSync();
                        log.debug("Оффсеты снапшотов зафиксированы");
                    } catch (Exception e) {
                        log.error("Ошибка при фиксации оффсетов снапшотов", e);
                    }
                } else if (hasErrors) {
                    log.warn("В текущей пачке снапшотов были ошибки, коммит оффсетов пропущен");
                }
            }
        } catch (WakeupException ignored) {
            log.info("Получен WakeupException, инициируем завершение работы процессора снапшотов");
        } catch (Exception e) {
            log.error("Необработанная ошибка в процессоре снапшотов", e);
        } finally {
            try {

                log.info("Завершение работы процессора снапшотов, закрываем consumer");
            } finally {
                snapshotConsumer.close();
            }
        }
    }
}