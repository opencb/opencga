package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Job {

    private int id;
    private String name;
    private String userId;
    private int analysisId;
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

    private String outdir;
    private List<Integer> input;    // input files to this job
    private List<Integer> output;   // output files of this job

    /**
     * To think about:
     * private Index index;
     */

    public static final String QUEUED = "queued";
    public static final String RUNNING = "running";
    public static final String DONE = "done";

    public Job() {
    }

    public Job(int id, String name, String userId, int analysisId, String toolName, String date, String description,
               long startTime, long endTime, String outputError, String commandLine, int visits, String status,
               long diskUsage, String outdir, List<Integer> input, List<Integer> output) {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.analysisId = analysisId;
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
        this.outdir = outdir;
        this.input = input;
        this.output = output;
    }

    @Override
    public String toString() {
        return "Job{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", userId='" + userId + '\'' +
                ", analysisId=" + analysisId +
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
                ", outdir='" + outdir + '\'' +
                ", input=" + input +
                ", output=" + output +
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

    public int getAnalysisId() {
        return analysisId;
    }

    public void setAnalysisId(int analysisId) {
        this.analysisId = analysisId;
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

    public String getOutdir() {
        return outdir;
    }

    public void setOutdir(String outdir) {
        this.outdir = outdir;
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
}
