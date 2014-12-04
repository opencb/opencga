package org.opencb.opencga.catalog.beans;

import org.opencb.opencga.lib.common.TimeUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Job {

    private int id;
    private String name;
    private String userId;
    private String toolName;
    private String date;
    private String description;
    private long startTime;
    private long endTime;
    private String outputError;
    private String commandLine;
    private int visits;
    private String status;
    private long diskUsage;

    private int outDirId;
    private URI tmpOutDirUri;

    private List<Integer> input;    // input files to this job
    private List<Integer> output;   // output files of this job

    private List<String> tags;
    private Map<String, Object> attributes;

    private Map<String, Object> resourceManagerAttributes;


    public static final String PREPARED = "prepared";
    public static final String QUEUED = "queued";
    public static final String RUNNING = "running";
    public static final String DONE = "done";       //Job finished, but output not ready
    public static final String READY = "ready";     //Job finished and ready
    public static final String ERROR = "error";     //Job finished with errors

    /* ResourceManagerAttributes known keys */
    public static final String TYPE = "type";
    public static final String TYPE_ANALYSIS = "analysis";
    public static final String TYPE_INDEX    = "index";
    public static final String JOB_SCHEDULER_NAME = "jobSchedulerName";
    public static final String INDEXED_FILE_ID = "indexedFileId";

    public Job() {
    }

    @Deprecated
    public Job(String name, String userId, String toolName, String description, String commandLine,
               List<Integer> input) {
        this(-1, name, userId, toolName, TimeUtils.getTime(), description, -1, -1, "", commandLine, -1, PREPARED, 0,
                -1, null, input, new LinkedList<Integer>(), new LinkedList<String>(), new HashMap<String, Object>(),
                new HashMap<String, Object>());
    }

    public Job(String name, String userId, String toolName, String description, String commandLine, int outDirId,
               URI tmpOutDirUri, List<Integer> input) {
        this(-1, name, userId, toolName, TimeUtils.getTime(), description, System.currentTimeMillis(), -1, "", commandLine, -1, PREPARED, 0,
                outDirId, tmpOutDirUri, input, new LinkedList<Integer>(), new LinkedList<String>(), new HashMap<String, Object>(),
                new HashMap<String, Object>());
    }

    public Job(int id, String name, String userId, String toolName, String date, String description,
               long startTime, long endTime, String outputError, String commandLine, int visits, String status,
               long diskUsage, int outDirId, URI tmpOutDirUri, List<Integer> input,
               List<Integer> output, List<String> tags, Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes) {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.toolName = toolName;
        this.date = date;
        this.description = description;
        this.startTime = startTime;
        this.endTime = endTime;
        this.outputError = outputError;
        this.commandLine = commandLine;
        this.visits = visits;
        this.status = status;
        this.diskUsage = diskUsage;
        this.outDirId = outDirId;
//        this.tmpOutDirId = tmpOutDirId;
        this.tmpOutDirUri = tmpOutDirUri;
//        this.outDir = outDir;
        this.input = input;
        this.output = output;
        this.tags = tags;
        this.attributes = attributes;
        this.resourceManagerAttributes = resourceManagerAttributes;

        //Initializing attributes maps.
        this.resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, "");
        this.resourceManagerAttributes.put(Job.TYPE, Job.TYPE_ANALYSIS);
    }

    @Override
    public String toString() {
        return "Job{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", userId='" + userId + '\'' +
                ", toolName='" + toolName + '\'' +
                ", date='" + date + '\'' +
                ", description='" + description + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", outputError='" + outputError + '\'' +
                ", commandLine='" + commandLine + '\'' +
                ", visits=" + visits +
                ", status='" + status + '\'' +
                ", diskUsage=" + diskUsage +
                ", outDirId=" + outDirId +
                ", tmpOutDirUri=" + tmpOutDirUri +
                ", input=" + input +
                ", output=" + output +
                ", tags=" + tags +
                ", attributes=" + attributes +
                ", resourceManagerAttributes=" + resourceManagerAttributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getToolName() {
        return toolName;
    }

    public void setToolName(String toolName) {
        this.toolName = toolName;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getOutputError() {
        return outputError;
    }

    public void setOutputError(String outputError) {
        this.outputError = outputError;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }

    public int getVisits() {
        return visits;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public int getOutDirId() {
        return outDirId;
    }

    public void setOutDirId(int outDirId) {
        this.outDirId = outDirId;
    }

    public URI getTmpOutDirUri() {
        return tmpOutDirUri;
    }

    public void setTmpOutDirUri(URI tmpOutDirUri) {
        this.tmpOutDirUri = tmpOutDirUri;
    }

    public List<Integer> getInput() {
        return input;
    }

    public void setInput(List<Integer> input) {
        this.input = input;
    }

    public List<Integer> getOutput() {
        return output;
    }

    public void setOutput(List<Integer> output) {
        this.output = output;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }


    public Map<String, Object> getResourceManagerAttributes() {
        return resourceManagerAttributes;
    }

    public void setResourceManagerAttributes(Map<String, Object> resourceManagerAttributes) {
        this.resourceManagerAttributes = resourceManagerAttributes;
    }
}
