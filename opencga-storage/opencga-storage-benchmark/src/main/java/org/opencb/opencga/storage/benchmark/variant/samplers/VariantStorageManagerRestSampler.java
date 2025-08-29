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

import org.apache.jmeter.protocol.http.util.HTTPArgument;
import org.opencb.opencga.core.api.ParamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStorageManagerRestSampler extends VariantStorageEngineRestSampler {

    public static final String STORAGE_MANAGER_REST_PATH = "/webservices/rest/v2/analysis/variant/query";
    private Logger logger = LoggerFactory.getLogger(getClass());

    public VariantStorageManagerRestSampler() {
    }

    public VariantStorageManagerRestSampler(URI uri) {
        super(uri);
    }

    public VariantStorageManagerRestSampler(String protocol, String host, String path, int port) {
        super(protocol, host, path, port);
    }

    @Override
    public VariantStorageManagerRestSampler setStorageEngine(String storageEngine) {
        // No need to set storage engine
        return this;
    }

    @Override
    public VariantStorageManagerRestSampler setDBName(String dbname) {
        // Set the project parameter instead of dbName
        getArguments().addArgument(new HTTPArgument(ParamConstants.PROJECT_PARAM, dbname, "="));
        return this;
    }
}
