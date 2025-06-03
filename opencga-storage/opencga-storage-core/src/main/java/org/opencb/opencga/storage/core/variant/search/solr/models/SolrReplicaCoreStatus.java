package org.opencb.opencga.storage.core.variant.search.solr.models;

public class SolrReplicaCoreStatus {
    private final String name;
    private final String type;
    private final String nodeName;
    private final String baseUrl;
    private final String coreName;
    private final String state;
    private final boolean leader;
    private final boolean preferredLeader;
    private final SolrCoreIndexStatus indexStatus;

    public SolrReplicaCoreStatus(String name, String type, String nodeName, String baseUrl, String coreName,
                                 String state, boolean leader, boolean preferredLeader, SolrCoreIndexStatus indexStatus) {
        this.name = name;
        this.type = type;
        this.nodeName = nodeName;
        this.baseUrl = baseUrl;
        this.coreName = coreName;
        this.state = state;
        this.leader = leader;
        this.preferredLeader = preferredLeader;
        this.indexStatus = indexStatus;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getCoreName() {
        return coreName;
    }

    public String getState() {
        return state;
    }

    public boolean isLeader() {
        return leader;
    }

    public boolean isPreferredLeader() {
        return preferredLeader;
    }

    public SolrCoreIndexStatus getIndexStatus() {
        return indexStatus;
    }
}
