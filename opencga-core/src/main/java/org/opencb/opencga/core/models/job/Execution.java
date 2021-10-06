package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;

import java.util.List;
import java.util.Map;

public class Execution extends PrivateStudyUid {

    private String id;
    private String uuid;
    private String description;

    private String userId;

    private String creationDate;
    private String modificationDate;

    private Enums.Priority priority;
    private ExecutionInternal internal;

    private Map<String, Object> params;

    private File outDir;
    //    private List<File> input;    // input files to this job
//    private List<File> output;   // output files of this job
    private List<String> tags;
    private List<Execution> dependsOn;

    private File stdout;
    private File stderr;

    private Pipeline pipeline;
    private boolean isPipeline;
    private boolean visited;

    private List<Job> jobs;

    private int release;
    private JobStudyParam study;
    private Map<String, Object> attributes;

    public Execution() {
    }

    public Execution(String id, String description, String userId, String creationDate, Map<String, Object> params, Enums.Priority priority,
                     List<String> tags, Pipeline pipeline, boolean isPipeline, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.userId = userId;
        this.creationDate = creationDate;
        this.params = params;
        this.priority = priority;
        this.tags = tags;
        this.pipeline = pipeline;
        this.isPipeline = isPipeline;
        this.attributes = attributes;
    }

    public Execution(long studyUid, String id, String uuid, String description, String userId, String creationDate, String modificationDate,
                     Map<String, Object> params, Enums.Priority priority, ExecutionInternal internal, File outDir, List<String> tags,
                     List<Execution> dependsOn, File stdout, File stderr, Pipeline pipeline, boolean isPipeline, boolean visited,
                     List<Job> jobs, int release, JobStudyParam study, Map<String, Object> attributes) {
        super(studyUid);
        this.id = id;
        this.uuid = uuid;
        this.description = description;
        this.userId = userId;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.params = params;
        this.priority = priority;
        this.internal = internal;
        this.outDir = outDir;
        this.tags = tags;
        this.dependsOn = dependsOn;
        this.stdout = stdout;
        this.stderr = stderr;
        this.pipeline = pipeline;
        this.isPipeline = isPipeline;
        this.visited = visited;
        this.jobs = jobs;
        this.release = release;
        this.study = study;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Execution{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", params='").append(params).append('\'');
        sb.append(", priority=").append(priority);
        sb.append(", internal=").append(internal);
        sb.append(", outDir=").append(outDir);
        sb.append(", tags=").append(tags);
        sb.append(", dependsOn=").append(dependsOn);
        sb.append(", stdout=").append(stdout);
        sb.append(", stderr=").append(stderr);
        sb.append(", pipeline=").append(pipeline);
        sb.append(", isPipeline=").append(isPipeline);
        sb.append(", visited=").append(visited);
        sb.append(", jobs=").append(jobs);
        sb.append(", release=").append(release);
        sb.append(", study=").append(study);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Execution setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public Execution setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Execution setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public Execution setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Execution setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Execution setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public Execution setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public Execution setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public ExecutionInternal getInternal() {
        return internal;
    }

    public Execution setInternal(ExecutionInternal internal) {
        this.internal = internal;
        return this;
    }

    public File getOutDir() {
        return outDir;
    }

    public Execution setOutDir(File outDir) {
        this.outDir = outDir;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public Execution setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public List<Execution> getDependsOn() {
        return dependsOn;
    }

    public Execution setDependsOn(List<Execution> dependsOn) {
        this.dependsOn = dependsOn;
        return this;
    }

    public File getStdout() {
        return stdout;
    }

    public Execution setStdout(File stdout) {
        this.stdout = stdout;
        return this;
    }

    public File getStderr() {
        return stderr;
    }

    public Execution setStderr(File stderr) {
        this.stderr = stderr;
        return this;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public Execution setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public boolean isIsPipeline() {
        return isPipeline;
    }

    public Execution setIsPipeline(boolean pipeline) {
        isPipeline = pipeline;
        return this;
    }

    public boolean isVisited() {
        return visited;
    }

    public Execution setVisited(boolean visited) {
        this.visited = visited;
        return this;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public Execution setJobs(List<Job> jobs) {
        this.jobs = jobs;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Execution setRelease(int release) {
        this.release = release;
        return this;
    }

    public JobStudyParam getStudy() {
        return study;
    }

    public Execution setStudy(JobStudyParam study) {
        this.study = study;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Execution setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
