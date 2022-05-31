package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Pipeline extends PrivateStudyUid {

    private String id;
    private String uuid;
    private String description;

    private boolean disabled;

    /**
     * An integer describing the current data release.
     *
     * @apiNote Immutable
     */
    @DataField(id = "release", managed = true, indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;
    private int version;

    private String creationDate;
    private String modificationDate;

    private PipelineInternal internal;

    private Map<String, Object> params;
    private PipelineConfig config;
    private LinkedHashMap<String, PipelineJob> jobs;

    public Pipeline() {
    }

    public Pipeline(String id, String description, boolean disabled, String creationDate, String modificationDate,
                    PipelineInternal internal, Map<String, Object> params, PipelineConfig config, LinkedHashMap<String, PipelineJob> jobs) {
        this.id = id;
        this.description = description;
        this.disabled = disabled;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.internal = internal;
        this.params = params;
        this.config = config;
        this.jobs = jobs;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Pipeline{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", disabled=").append(disabled);
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", internal=").append(internal);
        sb.append(", params=").append(params);
        sb.append(", config=").append(config);
        sb.append(", jobs=").append(jobs);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Pipeline setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public Pipeline setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Pipeline setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public Pipeline setDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Pipeline setVersion(int version) {
        this.version = version;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Pipeline setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public Pipeline setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public PipelineConfig getConfig() {
        return config;
    }

    public Pipeline setConfig(PipelineConfig config) {
        this.config = config;
        return this;
    }

    public LinkedHashMap<String, PipelineJob> getJobs() {
        return jobs;
    }

    public Pipeline setJobs(LinkedHashMap<String, PipelineJob> jobs) {
        this.jobs = jobs;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Pipeline setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Pipeline setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public PipelineInternal getInternal() {
        return internal;
    }

    public Pipeline setInternal(PipelineInternal internal) {
        this.internal = internal;
        return this;
    }

    public static class PipelineConfig {

        private int retry;
        private int timeout;

        public PipelineConfig() {
        }

        public PipelineConfig(int retry, int timeout) {
            this.retry = retry;
            this.timeout = timeout;
        }

        public static PipelineConfig init() {
            return new PipelineConfig(3, 3600);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Options{");
            sb.append("retry=").append(retry);
            sb.append(", timeout=").append(timeout);
            sb.append('}');
            return sb.toString();
        }

        public int getRetry() {
            return retry;
        }

        public PipelineConfig setRetry(int retry) {
            this.retry = retry;
            return this;
        }

        public int getTimeout() {
            return timeout;
        }

        public PipelineConfig setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    public static class PipelineJob {

        private String toolId;
        private PipelineJobExecutable executable;
        private String name;
        private String description;
        private Map<String, Object> params;
        private List<String> tags;
        private List<String> dependsOn;

        public PipelineJob() {
        }

        public PipelineJob(String toolId, PipelineJobExecutable executable, String name, String description, Map<String, Object> params,
                           List<String> tags, List<String> dependsOn) {
            this.toolId = toolId;
            this.executable = executable;
            this.name = name;
            this.description = description;
            this.params = params;
            this.tags = tags;
            this.dependsOn = dependsOn;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PipelineJob{");
            sb.append("toolId='").append(toolId).append('\'');
            sb.append(", executable=").append(executable);
            sb.append(", name='").append(name).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append(", params=").append(params);
            sb.append(", tags=").append(tags);
            sb.append(", dependsOn=").append(dependsOn);
            sb.append('}');
            return sb.toString();
        }

        public String getToolId() {
            return toolId;
        }

        public PipelineJob setToolId(String toolId) {
            this.toolId = toolId;
            return this;
        }

        public PipelineJobExecutable getExecutable() {
            return executable;
        }

        public PipelineJob setExecutable(PipelineJobExecutable executable) {
            this.executable = executable;
            return this;
        }

        public String getName() {
            return name;
        }

        public PipelineJob setName(String name) {
            this.name = name;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public PipelineJob setDescription(String description) {
            this.description = description;
            return this;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public PipelineJob setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        public List<String> getTags() {
            return tags;
        }

        public PipelineJob setTags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public List<String> getDependsOn() {
            return dependsOn;
        }

        public PipelineJob setDependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn;
            return this;
        }
    }

    public static class PipelineJobExecutable {

        private String id;
        private String command;

        public PipelineJobExecutable() {
        }

        public PipelineJobExecutable(String id, String command) {
            this.id = id;
            this.command = command;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PipelineJobExecutable{");
            sb.append("id='").append(id).append('\'');
            sb.append(", command='").append(command).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public PipelineJobExecutable setId(String id) {
            this.id = id;
            return this;
        }

        public String getCommand() {
            return command;
        }

        public PipelineJobExecutable setCommand(String command) {
            this.command = command;
            return this;
        }
    }
}
