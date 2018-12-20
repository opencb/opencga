package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.opencga.storage.benchmark.variant.queries.RandomQueries;

import java.util.Map;

/**
 * Created by wasim on 01/11/18.
 */
public abstract class ConfiguredQueryGenerator extends QueryGenerator {

    public void setUp(Map<String, String> params) {
        super.setUp(params);
    }

    public abstract void setUp(Map<String, String> params, RandomQueries configuration);

}
