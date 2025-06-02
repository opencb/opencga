package org.opencb.opencga.storage.core.variant.search.solr;

import java.util.List;

public class SolrShardStatus {
    private final String name;
    private final List<SolrReplicaCoreStatus> replicas;

    public SolrShardStatus(String name, List<SolrReplicaCoreStatus> replicas) {
        this.name = name;
        this.replicas = replicas;
    }

    public String getName() {
        return name;
    }

    public List<SolrReplicaCoreStatus> getReplicas() {
        return replicas;
    }
}
