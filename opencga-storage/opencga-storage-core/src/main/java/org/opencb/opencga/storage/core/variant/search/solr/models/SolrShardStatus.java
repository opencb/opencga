package org.opencb.opencga.storage.core.variant.search.solr.models;

import java.util.List;
import java.util.Objects;

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

    public long getSizeInBytes() {
        return replicas.stream()
                .map(SolrReplicaCoreStatus::getIndexStatus)
                .filter(Objects::nonNull)
                .mapToLong(SolrCoreIndexStatus::getSizeInBytes)
                .sum();
    }

    public long getNumDocs() {
        return replicas.stream()
                .filter(SolrReplicaCoreStatus::isLeader)
                .map(SolrReplicaCoreStatus::getIndexStatus)
                .filter(Objects::nonNull)
                .mapToLong(SolrCoreIndexStatus::getNumDocs)
                .findFirst().orElse(0L);
    }
}
