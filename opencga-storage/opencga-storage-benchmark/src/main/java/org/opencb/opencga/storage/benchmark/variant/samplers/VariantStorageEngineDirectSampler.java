/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.benchmark.variant.samplers;

import com.google.common.base.Throwables;
import org.apache.jmeter.config.Argument;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSampler;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.benchmark.variant.generators.QueryGenerator;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStorageEngineDirectSampler extends JavaSampler implements VariantStorageEngineSampler {

    private Logger logger = LogManager.getLogger(getClass());

    public VariantStorageEngineDirectSampler() {
        super();
        setClassname(VariantStorageEngineJavaSamplerClient.class.getName());
    }

    public VariantStorageEngineDirectSampler(String engine, String dbname) {
        this();
        setStorageEngine(engine);
        setDBName(dbname);
        setClassname(VariantStorageEngineJavaSamplerClient.class.getName());
    }

    @Override
    public VariantStorageEngineDirectSampler setStorageEngine(String engine) {
        getArguments().addArgument(new Argument(ENGINE, engine));
        return this;
    }

    @Override
    public VariantStorageEngineDirectSampler setDBName(String dbname) {
        getArguments().addArgument(new Argument(DB_NAME, dbname));
        return this;
    }

    @Override
    public VariantStorageEngineDirectSampler setLimit(int limit) {
        getArguments().addArgument(new Argument(QueryOptions.LIMIT, String.valueOf(limit)));
        return this;
    }

    @Override
    public VariantStorageEngineDirectSampler setCount(boolean count) {
        getArguments().addArgument(new Argument(QueryOptions.COUNT, String.valueOf(count)));
        return this;
    }

    @Override
    public VariantStorageEngineDirectSampler setQueryGenerator(Class<? extends QueryGenerator> queryGenerator) {
        getArguments().addArgument(new Argument(QUERY_GENERATOR, queryGenerator.getName()));
        return this;
    }

    @Override
    public VariantStorageEngineSampler setQueryGeneratorConfig(String key, String value) {
        getArguments().addArgument(new Argument(key, value));
        return this;
    }

    protected static class VariantStorageEngineJavaSamplerClient extends AbstractJavaSamplerClient {
        private Logger logger = LogManager.getLogger(getClass());
        private VariantStorageEngine variantStorageEngine;
        private QueryGenerator queryGenerator;

        public VariantStorageEngineJavaSamplerClient() {
        }

        @Override
        public void setupTest(JavaSamplerContext javaSamplerContext) {
            String engine = javaSamplerContext.getParameter(ENGINE);
            String dbName = javaSamplerContext.getParameter(DB_NAME);
            String queryGeneratorClazz = javaSamplerContext.getParameter(QUERY_GENERATOR);
            logger.debug("Using engine {}", engine);
            logger.debug("Using dbname {}", dbName);
            try {
                variantStorageEngine = StorageEngineFactory.get().getVariantStorageEngine(engine, dbName);
            } catch (Throwable e) {
                logger.error("Error creating VariantStorageEngine!", e);
                Throwables.propagate(e);
            }

            try {
                queryGenerator = (QueryGenerator) Class.forName(queryGeneratorClazz).newInstance();
                Map<String, String> params = new HashMap<>();
                javaSamplerContext.getParameterNamesIterator()
                        .forEachRemaining(name -> params.put(name, javaSamplerContext.getParameter(name)));
                queryGenerator.setUp(params);
                logger.debug("Using query generator {}", queryGenerator.getClass());
            } catch (Throwable e) {
                logger.error("Error creating QueryGenerator!", e);
                Throwables.propagate(e);
            }
            logger.debug("Setup finished!");

        }

        @Override
        public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

            SampleResult result = new SampleResult();

            try {
                Query query = queryGenerator.generateQuery(new Query());
                QueryOptions queryOptions = new QueryOptions(query);
                int limit = javaSamplerContext.getIntParameter(QueryOptions.LIMIT, -1);
                boolean count = Boolean.parseBoolean(javaSamplerContext.getParameter(QueryOptions.COUNT, Boolean.FALSE.toString()));

                if (limit > 0) {
                    queryOptions.append(QueryOptions.LIMIT, limit);
                }

                VariantQueryUtils.addDefaultLimit(queryOptions, variantStorageEngine.getOptions());

                result.setResponseMessage(query.toJson());
                result.setResponseCodeOK();
                result.sampleStart();
                long numResults = 0;
                if (count) {
                    numResults = variantStorageEngine.count(query).first();
                } else {
                    try (VariantDBIterator iterator = variantStorageEngine.iterator(query, queryOptions)) {
                        while (iterator.hasNext()) {
                            iterator.next();
                            numResults++;
                        }
                    }
                }
                result.sampleEnd();
                result.setBytes(numResults);
                result.setSuccessful(true);
                result.setSampleLabel(queryGenerator.getQueryId());

                logger.debug("query: {}", numResults);
            } catch (Error e) {
                result.setSuccessful(false);
                logger.error("Error!", e);
                throw e;
            } catch (Exception e) {
                logger.error("Error!", e);
                result.setSuccessful(false);
                result.setErrorCount(1);
            }
            return result;
        }

        @Override
        public void teardownTest(JavaSamplerContext javaSamplerContext) {
            logger.debug("Closing variant engine");
            try {
                variantStorageEngine.close();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
    }
}
