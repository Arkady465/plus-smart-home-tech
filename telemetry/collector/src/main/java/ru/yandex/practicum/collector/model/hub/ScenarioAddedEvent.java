package ru.yandex.practicum.collector.model.hub;

import java.util.List;

public class ScenarioAddedEvent extends HubEvent {

    private String name;
    private List<ScenarioCondition> conditions;
    private List<DeviceAction> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }

    public String getName() {
        return name;
    }

    public List<ScenarioCondition> getConditions() {
        return conditions;
    }

    public List<DeviceAction> getActions() {
        return actions;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setConditions(List<ScenarioCondition> conditions) {
        this.conditions = conditions;
    }

    public void setActions(List<DeviceAction> actions) {
        this.actions = actions;
    }
}
