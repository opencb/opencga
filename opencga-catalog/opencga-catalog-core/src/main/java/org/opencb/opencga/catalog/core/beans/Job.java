package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Job {
    private String date;
    private String status;
    private String toolName;
    private long diskUsage;
    private String startTime;
    private String endTime;
    private String ouputError;
    private int visits;
    private String commandLine;
    private String description;

    private List<Integer> input;    //fileId
    private List<Integer> output;   //fileId

    public static final String QUEUED = "queued";
    public static final String RUNNING = "running";
    public static final String DONE = "done";

    public Job() {
    }

    public Job(String date, String status, String toolName, long diskUsage, String startTime, String endTime, String ouputError, int visits, String commandLine, String description, List<Integer> input, List<Integer> output) {
        this.date = date;
        this.status = status;
        this.toolName = toolName;
        this.diskUsage = diskUsage;
        this.startTime = startTime;
        this.endTime = endTime;
        this.ouputError = ouputError;
        this.visits = visits;
        this.commandLine = commandLine;
        this.description = description;
        this.input = input;
        this.output = output;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getToolName() {
        return toolName;
    }

    public void setToolName(String toolName) {
        this.toolName = toolName;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getOuputError() {
        return ouputError;
    }

    public void setOuputError(String ouputError) {
        this.ouputError = ouputError;
    }

    public int getVisits() {
        return visits;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
