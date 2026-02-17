package ru.yandex.practicum;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final SnapshotStorage snapshotStorage;
    @Value("${kafka.input-topic}")
    private String inputTopic;
    @Value("${kafka.output-topic}")
    private String outputTopic;

    public void start() {
        try {
            consumer.subscribe(List.of(inputTopic));
            log.info("Подписка на топик {}", inputTopic);

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                boolean hasErrors = false; // флаг ошибок при обработке текущей пачки

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    try {
                        Optional<SensorsSnapshotAvro> mayBeSnapshot = snapshotStorage.updateState(record.value());

                        if (mayBeSnapshot.isPresent()) {
                            SensorsSnapshotAvro snapshot = mayBeSnapshot.get();
                            ProducerRecord<String, SensorsSnapshotAvro> producerRecord =
                                    new ProducerRecord<>(outputTopic, snapshot.getHubId().toString(), snapshot);

                            // Синхронная отправка с ожиданием результата
                            try {
                                producer.send(producerRecord).get(); // блокируем до завершения отправки
                                log.debug("Снапшот для hubId={} отправлен в топик {}", snapshot.getHubId(), outputTopic);
                            } catch (InterruptedException | ExecutionException e) {
                                log.error("Ошибка при отправке снапшота в Kafka: {}", e.getMessage(), e);
                                throw new RuntimeException("Ошибка отправки в Kafka", e); // пробрасываем, чтобы запись считалась ошибочной
                            }
                        }
                    } catch (Exception e) {
                        log.error("Ошибка при обработке записи: ключ={}, значение={}", record.key(), record.value(), e);
                        hasErrors = true; // помечаем, что в этой пачке была ошибка
                    }
                }

                // Коммитим оффсеты только если не было ошибок
                if (!hasErrors) {
                    try {
                        consumer.commitSync();
                        log.debug("Оффсеты успешно зафиксированы");
                    } catch (Exception e) {
                        log.error("Ошибка при фиксации оффсетов", e);

                    }
                } else {
                    log.warn("В текущей пачке были ошибки обработки, коммит оффсетов пропущен");
                }
            }

        } catch (WakeupException ignored) {
            log.info("Получен WakeupException, начинаем завершение работы");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                // Даем время на завершение отправки всех сообщений
                producer.flush();
                log.info("Все данные отправлены в Kafka");

                // При завершении пытаемся закоммитить последние оффсеты, если consumer не закрыт
                if (consumer != null) {
                    consumer.commitSync();
                    log.info("Все смещения зафиксированы при завершении");
                }
            } catch (Exception e) {
                log.error("Ошибка при завершении работы", e);
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close(Duration.ofSeconds(10));
                log.info("Закрываем продюсер");
                producer.close(Duration.ofSeconds(30));
            }
        }
    }
}