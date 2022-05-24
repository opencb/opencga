package org.opencb.opencga.test.config;

import java.util.List;

public class Execution {


    private String id;
    private String queue;
    private String azureCredentials;
    private List<String> options;

    public Execution() {
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Execution{\n");
        sb.append("id='").append(id).append('\'').append("\n");
        sb.append("queue='").append(queue).append('\'').append("\n");
        sb.append("azureCredentials='").append(azureCredentials).append('\'').append("\n");
        sb.append("options=").append(options).append("\n");
        sb.append("}\n");
        return sb.toString();
    }


    public String getId() {
        return id;
    }

    public Execution setId(String id) {
        this.id = id;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public Execution setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public String getAzureCredentials() {
        return azureCredentials;
    }

    public Execution setAzureCredentials(String azureCredentials) {
        this.azureCredentials = azureCredentials;
        return this;
    }

    public List<String> getOptions() {
        return options;
    }

    public Execution setOptions(List<String> options) {
        this.options = options;
        return this;
    }
}
