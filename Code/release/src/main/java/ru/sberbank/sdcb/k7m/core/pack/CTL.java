package ru.sberbank.sdcb.k7m.core.pack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties({"global"})
public class CTL {
    private List<Entity> entities;
    private List<Map<String, Workflow>> workflows;

    public List<Entity> getEntities() {
        return entities;
    }

    public void setEntities(List<Entity> entities) {
        this.entities = entities;
    }

    public List<Map<String, Workflow>> getWorkflows() {
        return workflows;
    }

    public void setWorkflows(List<Map<String, Workflow>> workflows) {
        this.workflows = workflows;
    }
}
