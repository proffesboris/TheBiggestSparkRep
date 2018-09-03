package ru.sberbank.sdcb.k7m.java;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

public class Statval implements Serializable {

    @JsonProperty("loading_id")
    private String loadingId;

    @JsonProperty("entity_id")
    private String entityId;

    @JsonProperty("stat_id")
    private String statId;

    @JsonProperty("value")
    private Date value;

    public String getLoadingId() {
        return loadingId;
    }

    public void setLoadingId(String loadingId) {
        this.loadingId = loadingId;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getStatId() {
        return statId;
    }

    public void setStatId(String statId) {
        this.statId = statId;
    }

    public Date getValue() {
        return value;
    }

    public void setValue(Date value) {
        this.value = value;
    }
}
