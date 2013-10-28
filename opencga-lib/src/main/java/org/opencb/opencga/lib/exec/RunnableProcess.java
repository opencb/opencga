package org.opencb.opencga.lib.exec;


public abstract class RunnableProcess implements Runnable {

    protected long startTime;
    protected long endTime;

    protected String output;
    protected String error;

    protected String exception;
    protected int exitValue;

    protected Status status;

    public enum Status {WAITING, RUNNING, DONE, ERROR, TIMEOUT, KILLED}

    protected void startTime() {
        startTime = System.currentTimeMillis();
    }

    protected void endTime() {
        endTime = System.currentTimeMillis();
    }

    protected int getDuration() {
        return (int) (endTime - getStartTime());
    }

    protected abstract void destroy();

    /**
     * @return the output
     */
    public String getOutput() {
        return output;
    }

    /**
     * @param output the output to set
     */
    public void setOutput(String output) {
        this.output = output;
    }

    /**
     * @return the error
     */
    public String getError() {
        return error;
    }

    /**
     * @param error the error to set
     */
    public void setError(String error) {
        this.error = error;
    }

    /**
     * @return the exception
     */
    public String getException() {
        return exception;
    }

    /**
     * @param exception the exception to set
     */
    public void setException(String exception) {
        this.exception = exception;
    }

    /**
     * @return the exitValue
     */
    public int getExitValue() {
        return exitValue;
    }

    /**
     * @param exitValue the exitValue to set
     */
    public void setExitValue(int exitValue) {
        this.exitValue = exitValue;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param startTime the startTime to set
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return the startTime
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @param endTime the endTime to set
     */
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /**
     * @return the endTime
     */
    public long getEndTime() {
        return endTime;
    }

}
