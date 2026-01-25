package ru.yandex.practicum.collector.model.hub;

public class ScenarioRemovedEvent extends HubEvent {

    private String name;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
