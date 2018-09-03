package ru.sberbank.sdcb.k7m.core.pack;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties({"init_locks", "schedule"})
public class Workflow {

    private String name;
    private String category;
    private String type;
    private String orchestrator;
    private ScheduleParams schedule_params;
    private List<Map<String, Param>> params;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOrchestrator() {
        return orchestrator;
    }

    public void setOrchestrator(String orchestrator) {
        this.orchestrator = orchestrator;
    }

    public ScheduleParams getSchedule_params() {
        return schedule_params;
    }

    public void setSchedule_params(ScheduleParams schedule_params) {
        this.schedule_params = schedule_params;
    }

    public List<Map<String, Param>> getParams() {
        return params;
    }

    public void setParams(List<Map<String, Param>> params) {
        this.params = params;
    }

    @JsonIgnoreProperties({"eventAwaitStrategy", "cron"})
    public static class ScheduleParams {
        private List<Map<String, Schedule>> entities;

        public List<Map<String, Schedule>> getEntities() {
            return entities;
        }

        public void setEntities(List<Map<String, Schedule>> entities) {
            this.entities = entities;
        }
    }

    public static class Schedule {
        private String id;
        private String statisticId;
        private String active;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getStatisticId() {
            return statisticId;
        }

        public void setStatisticId(String statisticId) {
            this.statisticId = statisticId;
        }

        public String getActive() {
            return active;
        }

        public void setActive(String active) {
            this.active = active;
        }
    }

    public static class Param {
        private String name;
        private String prior_value;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPrior_value() {
            return prior_value;
        }

        public void setPrior_value(String prior_value) {
            this.prior_value = prior_value;
        }
    }

}
