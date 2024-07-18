package org.opencb.opencga.core.models.nextflow;

import org.opencb.opencga.core.models.PrivateFields;

import java.util.List;

public class Workflow extends PrivateFields {

    private String id;
    private String uuid;
    private int version;
    private Type type;
    private String commandLine;
    private List<Script> scripts;

    private String creationDate;
    private String modificationDate;

    public enum Type {
        NEXTFLOW
    }

    public Workflow() {
    }

    public Workflow(String id, int version, Type type, String commandLine, List<Script> scripts, String creationDate,
                    String modificationDate) {
        this.id = id;
        this.version = version;
        this.type = type;
        this.commandLine = commandLine;
        this.scripts = scripts;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Workflow{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", version=").append(version);
        sb.append(", type=").append(type);
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", scripts=").append(scripts);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Workflow setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Workflow setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Workflow setVersion(int version) {
        this.version = version;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Workflow setType(Type type) {
        this.type = type;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public Workflow setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public List<Script> getScripts() {
        return scripts;
    }

    public Workflow setScripts(List<Script> scripts) {
        this.scripts = scripts;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Workflow setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Workflow setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public static class Script {

        private String id;
        private String content;

        public Script() {
        }

        public Script(String id, String content) {
            this.id = id;
            this.content = content;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Script{");
            sb.append("id='").append(id).append('\'');
            sb.append(", content='").append(content).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public Script setId(String id) {
            this.id = id;
            return this;
        }

        public String getContent() {
            return content;
        }

        public Script setContent(String content) {
            this.content = content;
            return this;
        }
    }

}
