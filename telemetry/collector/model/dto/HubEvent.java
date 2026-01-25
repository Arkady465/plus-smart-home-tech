package ru.yandex.practicum.kafka.telemetry.collector.model.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.kafka.telemetry.collector.model.HubEventType;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        defaultImpl = HubEvent.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DeviceAddedEvent.class, name = "DEVICE_ADDED_EVENT"),
        @JsonSubTypes.Type(value = DeviceRemovedEvent.class, name = "DEVICE_REMOVED_EVENT"),
        @JsonSubTypes.Type(value = ScenarioAddedEvent.class, name = "SCENARIO_ADDED_EVENT"),
        @JsonSubTypes.Type(value = ScenarioRemovedEvent.class, name = "SCENARIO_REMOVED_EVENT")
})
@Getter
@Setter
@ToString
public abstract class HubEvent {
    @NotBlank(message = "Hub id cannot be blank")
    private String hubId;

    @NotNull(message = "Timestamp cannot be null")
    private Instant timestamp = Instant.now();

    @NotNull(message = "Event type cannot be null")
    public abstract HubEventType getType();
}