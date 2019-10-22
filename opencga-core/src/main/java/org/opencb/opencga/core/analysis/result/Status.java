package org.opencb.opencga.core.analysis.result;

import java.util.Date;

public class Status {

    /**
     * NONE status: The job or step has not started yet.
     */
    public static final String NONE = "NONE";

    /**
     * SKIP status: The job or step has been skipped.
     */
    public static final String SKIP = "SKIP";

    /**
     * RUNNING status: The job or step is running.
     */
    public static final String RUNNING = "RUNNING";

    /**
     * DONE status: The job or step has finished the execution, but the output is still not ready.
     */
    public static final String DONE = "DONE";
    /**
     * ERROR status: The job or step finished with an error.
     */
    public static final String ERROR = "ERROR";

    private String id;
    private String step;
    private Date date;

    public Status() {
    }

    public Status(String id, String step, Date date) {
        this.id = id;
        this.step = step;
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

    public Date getDate() {
        return date;
    }

    public Status setDate(Date date) {
        this.date = date;
        return this;
    }
}
