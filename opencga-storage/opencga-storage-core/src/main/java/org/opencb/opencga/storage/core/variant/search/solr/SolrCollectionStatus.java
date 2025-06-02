package org.opencb.opencga.storage.core.variant.search.solr;

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
}
