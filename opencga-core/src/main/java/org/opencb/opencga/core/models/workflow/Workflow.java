package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateFields;

import java.util.List;
import java.util.Map;

public class Workflow extends PrivateFields {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.WORKFLOW_ID_DESCRIPTION)
    private String id;

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "version", managed = true, indexed = true,
            description = FieldConstants.GENERIC_VERSION_DESCRIPTION)
    private int version;

    @DataField(id = "type", description = FieldConstants.WORKFLOW_TYPE_DESCRIPTION)
    private Type type;

    @DataField(id = "commandLine", description = FieldConstants.WORKFLOW_COMMAND_LINE_DESCRIPTION)
    private String commandLine;

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<Script> scripts;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public enum Type {
        NEXTFLOW
    }

    public Workflow() {
    }

    public Workflow(String id, String description, int version, Type type, String commandLine, List<Script> scripts, String creationDate,
                    String modificationDate, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.type = type;
        this.commandLine = commandLine;
        this.scripts = scripts;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Workflow{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", version=").append(version);
        sb.append(", type=").append(type);
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", scripts=").append(scripts);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
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

    public String getDescription() {
        return description;
    }

    public Workflow setDescription(String description) {
        this.description = description;
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Workflow setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
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
