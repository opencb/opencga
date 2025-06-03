package org.opencb.opencga.storage.core.variant.search.solr.models;

import java.util.List;

public class SolrCollectionStatus {
    private final String name;
    private final List<SolrShardStatus> shards;

    public SolrCollectionStatus(String name, List<SolrShardStatus> shards) {
        this.name = name;
        this.shards = shards;
    }

    public String getName() {
        return name;
    }

    public List<SolrShardStatus> getShards() {
        return shards;
    }

    public long getSizeInBytes() {
        return shards.stream()
                .mapToLong(SolrShardStatus::getSizeInBytes)
                .sum();
    }

    public long getNumDocs() {
        return shards.stream()
                .mapToLong(SolrShardStatus::getNumDocs)
                .sum();
    }
}
