package org.opencb.opencga.catalog.core.beans;

import org.opencb.opencga.lib.common.TimeUtils;

/**
 * Created by imedina on 13/09/14.
 */
public class Metadata {

    private String version;
    private String date;
    private String open;

    private int projectCounter;
    private int studyCounter;
    private int fileCounter;
    private int analysisCounter;
    private int jobCounter;
    private int sampleCounter;

    public Metadata() {
        this("v2", TimeUtils.getTime(), "public");
    }

    public Metadata(String version, String date, String open) {
        this(version, date, open, 0, 0, 0, 0, 0, 0);
    }

    public Metadata(String version, String date, String open, int projectCounter, int studyCounter, int fileCounter, int analysisCounter, int jobCounter, int sampleCounter) {
        this.version = version;
        this.date = date;
        this.open = open;
        this.projectCounter = projectCounter;
        this.studyCounter = studyCounter;
        this.fileCounter = fileCounter;
        this.analysisCounter = analysisCounter;
        this.jobCounter = jobCounter;
        this.sampleCounter = sampleCounter;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "version='" + version + '\'' +
                ", date='" + date + '\'' +
                ", open='" + open + '\'' +
                ", projectCounter=" + projectCounter +
                ", studyCounter=" + studyCounter +
                ", fileCounter=" + fileCounter +
                ", analysisCounter=" + analysisCounter +
                ", jobCounter=" + jobCounter +
                ", sampleCounter=" + sampleCounter +
                '}';
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getProjectCounter() {
        return projectCounter;
    }

    public void setProjectCounter(int projectCounter) {
        this.projectCounter = projectCounter;
    }

    public int getStudyCounter() {
        return studyCounter;
    }

    public void setStudyCounter(int studyCounter) {
        this.studyCounter = studyCounter;
    }

    public int getFileCounter() {
        return fileCounter;
    }

    public void setFileCounter(int fileCounter) {
        this.fileCounter = fileCounter;
    }

    public int getAnalysisCounter() {
        return analysisCounter;
    }

    public void setAnalysisCounter(int analysisCounter) {
        this.analysisCounter = analysisCounter;
    }

    public int getJobCounter() {
        return jobCounter;
    }

    public void setJobCounter(int jobCounter) {
        this.jobCounter = jobCounter;
    }

    public int getSampleCounter() {
        return sampleCounter;
    }

    public void setSampleCounter(int sampleCounter) {
        this.sampleCounter = sampleCounter;
    }

    public String getOpen() {
        return open;
    }

    public void setOpen(String open) {
        this.open = open;
    }
}
