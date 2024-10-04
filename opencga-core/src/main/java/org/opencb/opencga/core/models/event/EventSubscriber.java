package org.opencb.opencga.core.models.event;

public class EventSubscriber {

    private String id;
    private boolean successful;
    private int numAttempts;

    public EventSubscriber() {
    }

    public EventSubscriber(String id, boolean successful, int numAttempts) {
        this.id = id;
        this.successful = successful;
        this.numAttempts = numAttempts;
    }

    public String getId() {
        return id;
    }

    public EventSubscriber setId(String id) {
        this.id = id;
        return this;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public EventSubscriber setSuccessful(boolean successful) {
        this.successful = successful;
        return this;
    }

    public int getNumAttempts() {
        return numAttempts;
    }

    public EventSubscriber setNumAttempts(int numAttempts) {
        this.numAttempts = numAttempts;
        return this;
    }
}
