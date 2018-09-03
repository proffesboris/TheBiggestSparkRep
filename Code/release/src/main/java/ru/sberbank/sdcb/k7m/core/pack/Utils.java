package ru.sberbank.sdcb.k7m.core.pack;

import java.util.*;

public class Utils {

    public static final String ENTITY = "entity";

    public static String getEntityId(Workflow workflow) {

        for (Map<String, Workflow.Param> paramMap : workflow.getParams()) {
            for (Workflow.Param param : paramMap.values()) {
                if (ENTITY.equals(param.getName())) {
                    return param.getPrior_value();
                }
            }
        }
        throw new IllegalStateException();
    }


    public static Set<String> getParentsIds(Workflow workflow) {
        Workflow.ScheduleParams schedule_params = workflow.getSchedule_params();

        if (schedule_params == null) {
            return Collections.emptySet();
        }
        List<Map<String, Workflow.Schedule>> entities = schedule_params.getEntities();
        if (entities == null) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (Map<String, Workflow.Schedule> entity : entities) {
            for (Workflow.Schedule schedule : entity.values()) {
                result.add(schedule.getId());
            }
        }
        return result;
    }

    public static Set<String> getParentNames(Map<String, Workflow> workflows, Set<String> parentIds) {
        Set<String> result = new HashSet<>();
        for (Workflow workflow : workflows.values()) {
            String entityId = getEntityId(workflow);
            if (parentIds.contains(entityId)) {
                result.add(workflow.getName());
            }
        }
        return result;
    }

    public static Info build(CTL ctl) {
        Info info = new Info();
        info.workflowByName = new HashMap<>();
        info.parentsMap = new HashMap<>();
        for (Map<String, Workflow> workflows : ctl.getWorkflows()) {
            for (Workflow workflow : workflows.values()) {
                info.workflowByName.put(workflow.getName(), workflow);
            }
        }
        for (Workflow workflow : info.workflowByName.values()) {
            Set<String> parentsIds = getParentsIds(workflow);
            if (!parentsIds.isEmpty()) {
                info.parentsMap.put(workflow.getName(), getParentNames(info.workflowByName, parentsIds));
            }
        }
        return info;
    }

    public static class Info {
        public Map<String, Workflow> workflowByName;
        public Map<String, Set<String>> parentsMap;
    }

}
