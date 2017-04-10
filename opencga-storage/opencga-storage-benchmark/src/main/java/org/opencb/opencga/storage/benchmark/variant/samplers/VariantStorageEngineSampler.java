package org.opencb.opencga.storage.benchmark.variant.samplers;

import org.apache.jmeter.samplers.Sampler;
import org.opencb.opencga.storage.benchmark.variant.generators.QueryGenerator;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface VariantStorageEngineSampler extends Sampler {

    String ENGINE = "engine";
    String DB_NAME = "dbName";
    String QUERY_GENERATOR = "queryGenerator";

    VariantStorageEngineSampler setStorageEngine(String engine);

    VariantStorageEngineSampler setDBName(String dbname);

    VariantStorageEngineSampler setLimit(int limit);

    VariantStorageEngineSampler setCount(boolean count);

    VariantStorageEngineSampler setQueryGenerator(Class<? extends QueryGenerator> queryGenerator);

    VariantStorageEngineSampler setQueryGeneratorConfig(String key, String value);

}
