package ru.sberbank.sdcb.k7m.core.pack;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Entity {

    private Inner entity;

    public Inner getEntity() {
        return entity;
    }

    public void setEntity(Inner entity) {
        this.entity = entity;
    }

    public static class Inner {

        private String id;
        private String name;
        private String path;
        private String storage;
        @JsonProperty("parent-id")
        private String parentId;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getStorage() {
            return storage;
        }

        public void setStorage(String storage) {
            this.storage = storage;
        }

        public String getParentId() {
            return parentId;
        }

        public void setParentId(String parentId) {
            this.parentId = parentId;
        }
    }

}


