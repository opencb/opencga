package org.opencb.opencga.core.config;

public class Batch {

    private String id;
    private String queue;
    private int timeout;

    public Batch() {
    }

    public Batch(String id, String queue, int timeout) {
        this.id = id;
        this.queue = queue;
        this.timeout = timeout;
    }

    public String getId() {
        return id;
    }

    public Batch setId(String id) {
        this.id = id;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public Batch setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public Batch setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }
}
