/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.clinical.rd;

import org.opencb.opencga.core.common.JacksonUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class RdInterpretationConfiguration {

    private Map<String, Object> queries;
    private TierConfiguration tierConfiguration;

    public RdInterpretationConfiguration() {
    }

    public RdInterpretationConfiguration(Map<String, Object> queries, TierConfiguration tierConfiguration) {
        this.queries = queries;
        this.tierConfiguration = tierConfiguration;
    }

    public static RdInterpretationConfiguration load(Path path) throws IOException {
        return load(Files.newInputStream(path));
    }

    public static RdInterpretationConfiguration load(InputStream inputStream) {
        return JacksonUtils.getDefaultObjectMapper().convertValue(new Yaml().load(inputStream), RdInterpretationConfiguration.class);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RdInterpretationConfiguration{");
        sb.append(", queries=").append(queries);
        sb.append(", tierConfiguration=").append(tierConfiguration);
        sb.append('}');
        return sb.toString();
    }

    public Map<String, Object> getQueries() {
        return queries;
    }

    public RdInterpretationConfiguration setQueries(Map<String, Object> queries) {
        this.queries = queries;
        return this;
    }

    public TierConfiguration getTierConfiguration() {
        return tierConfiguration;
    }

    public RdInterpretationConfiguration setTierConfiguration(TierConfiguration tierConfiguration) {
        this.tierConfiguration = tierConfiguration;
        return this;
    }
}
