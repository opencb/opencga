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
import org.apache.jmeter.protocol.http.sampler.HTTPSampleResult;
import org.apache.jmeter.protocol.http.sampler.HTTPSampler;
import org.apache.jmeter.protocol.http.util.HTTPArgument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.benchmark.variant.generators.QueryGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStorageEngineRestSampler extends HTTPSampler implements VariantStorageEngineSampler {

    public static final String REST_PATH = "/webservices/rest/v1/analysis/variant/query";
    public static final String STORAGE_REST_PATH = "/webservices/rest/variants/query";
    private Logger logger = LogManager.getLogger(getClass());
    private QueryGenerator queryGenerator;

    public VariantStorageEngineRestSampler() {
        setPath(STORAGE_REST_PATH);
        setMethod("GET");
    }

    public VariantStorageEngineRestSampler(String host, int port) {
        this();
        setDomain(host);
        setPort(port);
    }

    public VariantStorageEngineRestSampler(String host, String path, int port) {
        setDomain(host);
        setPort(port);
        setPath(path);
        setMethod("GET");
    }

    @Override
    public String getQueryString(String contentEncoding) {
        StringBuilder sb = new StringBuilder(super.getQueryString(contentEncoding));

        if (sb.length() != 0) {
            sb.append('&');
        }

        Query query = getQueryGenerator().generateQuery(new Query());
        query.forEach((key, value) -> sb.append(key).append('=').append(value).append('&'));
        return encodedString(sb.toString());
    }

    @Override
    protected HTTPSampleResult sample(java.net.URL u, String method, boolean areFollowingRedirect, int depth) {
        HTTPSampleResult sample = super.sample(u, method, areFollowingRedirect, depth);
        sample.setSampleLabel(queryGenerator.getQueryId());

        if (getArguments().getArgumentsAsMap().get(QueryOptions.COUNT).equalsIgnoreCase("true")) {
            String str = sample.getResponseDataAsString();
            sample.setResponseMessage(str.substring(str.lastIndexOf("[") + 1, str.indexOf("]")));
        }
        return sample;
    }

    @Override
    public VariantStorageEngineRestSampler setStorageEngine(String storageEngine) {
        getArguments().addArgument(new HTTPArgument("storageEngine", storageEngine, "="));
        return this;
    }

    @Override
    public VariantStorageEngineRestSampler setDBName(String dbname) {
        getArguments().addArgument(new HTTPArgument("dbName", dbname, "="));
        return this;
    }

    @Override
    public VariantStorageEngineRestSampler setLimit(int limit) {
        getArguments().addArgument(new HTTPArgument(QueryOptions.LIMIT, String.valueOf(limit), "="));
        return this;
    }

    @Override
    public VariantStorageEngineSampler setCount(boolean count) {
        getArguments().addArgument(new HTTPArgument(QueryOptions.COUNT, String.valueOf(count), "="));
        return this;
    }

    @Override
    public VariantStorageEngineRestSampler setQueryGenerator(Class<? extends QueryGenerator> queryGenerator) {
        setProperty("VariantStorageEngineSamples." + VariantStorageEngineSampler.QUERY_GENERATOR, queryGenerator.getName());
        return this;
    }

    private String getQueryGeneratorClassName() {
        return getPropertyAsString("VariantStorageEngineSamples." + VariantStorageEngineSampler.QUERY_GENERATOR);
    }

    @Override
    public VariantStorageEngineSampler setQueryGeneratorConfig(String key, String value) {
        setProperty(QueryGenerator.class.getName() + '.' + key, value);
        return this;
    }

    private QueryGenerator getQueryGenerator() {
        if (Objects.isNull(queryGenerator)) {
            String queryGeneratorClassName = getQueryGeneratorClassName();
            try {
                Map<String, String> map = new HashMap<>();
                String queryGeneratorProperties = QueryGenerator.class.getName() + ".";
                propertyIterator().forEachRemaining(property -> {
                    if (property.getName().startsWith(queryGeneratorProperties)) {
                        map.put(property.getName().substring(queryGeneratorProperties.length()), property.getStringValue());
                    }
                });
                queryGenerator = (QueryGenerator) Class.forName(queryGeneratorClassName).newInstance();
                queryGenerator.setUp(map);
                logger.debug("Using query generator " + queryGenerator.getClass());
            } catch (Throwable e) {
                logger.error("Error creating QueryGenerator!", e);
                Throwables.propagate(e);
            }
        }
        return queryGenerator;
    }

    private String encodedString(String string) {
        string = string.replaceAll(">=", "%3E%3D").replaceAll("<=", "%3C%3D");
        return string.replaceAll(">", "%3E").replaceAll("<", "%3C");
    }
}
