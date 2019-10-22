package org.opencb.opencga.core.analysis.result;

import java.util.Date;

public class Status {
    private String id;
    private String step;
    private float completedPercentage;
    private Date date;

    public Status() {
    }

    public Status(String id, String step, float completedPercentage, Date date) {
        this.id = id;
        this.step = step;
        this.completedPercentage = completedPercentage;
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public Status setId(String id) {
        this.id = id;
        return this;
    }

    public String getStep() {
        return step;
    }

    public Status setStep(String step) {
        this.step = step;
        return this;
    }

    public float getCompletedPercentage() {
        return completedPercentage;
    }

    public Status setCompletedPercentage(float completedPercentage) {
        this.completedPercentage = completedPercentage;
        return this;
    }

    public Date getDate() {
        return date;
    }

    public Status setDate(Date date) {
        this.date = date;
        return this;
    }
}
