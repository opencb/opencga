package org.opencb.opencga.core.analysis.result;

import java.util.Date;

public class Status {

    public enum Type {
        /**
         * PENDING status: The job or step has not started yet.
         */
         PENDING,

        /**
         * RUNNING status: The job or step is running.
         */
         RUNNING,

        /**
         * DONE status: The job or step has finished the execution, but the output is still not ready.
         */
         DONE,

        /**
         * ERROR status: The job or step finished with an error.
         */
         ERROR
    }

    private Type name;
    private String step;
    private Date date;

    public Status() {
    }

    public Status(Type name, String step, Date date) {
        this.name = name;
        this.step = step;
        this.date = date;
    }

    public Type getName() {
        return name;
    }

    public Status setName(Type name) {
        this.name = name;
        return this;
    }

    public String getStep() {
        return step;
    }

    public Status setStep(String step) {
        this.step = step;
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
