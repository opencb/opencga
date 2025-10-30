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

package org.opencb.opencga.analysis.clinical;

import org.opencb.opencga.core.common.JacksonUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ClinicalInterpretationConfiguration {

    private String penetrance;
    private Map<String, Object> queries;
    private Boolean oneConsequencePerEvidence;
    private TierConfiguration tierConfiguration;

    public ClinicalInterpretationConfiguration() {
    }

    public ClinicalInterpretationConfiguration(String penetrance, Map<String, Object> queries, Boolean oneConsequencePerEvidence,
                                               TierConfiguration tierConfiguration) {
        this.penetrance = penetrance;
        this.queries = queries;
        this.oneConsequencePerEvidence = oneConsequencePerEvidence;
        this.tierConfiguration = tierConfiguration;
    }

    public static ClinicalInterpretationConfiguration load(Path path) throws IOException {
        return load(Files.newInputStream(path));
    }

    public static ClinicalInterpretationConfiguration load(InputStream inputStream) {
        return JacksonUtils.getDefaultObjectMapper().convertValue(new Yaml().load(inputStream), ClinicalInterpretationConfiguration.class);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalInterpretationConfiguration{");
        sb.append("penetrance='").append(penetrance).append('\'');
        sb.append(", queries=").append(queries);
        sb.append(", oneConsequencePerEvidence=").append(oneConsequencePerEvidence);
        sb.append(", tierConfiguration=").append(tierConfiguration);
        sb.append('}');
        return sb.toString();
    }

    public String getPenetrance() {
        return penetrance;
    }

    public ClinicalInterpretationConfiguration setPenetrance(String penetrance) {
        this.penetrance = penetrance;
        return this;
    }

    public Map<String, Object> getQueries() {
        return queries;
    }

    public ClinicalInterpretationConfiguration setQueries(Map<String, Object> queries) {
        this.queries = queries;
        return this;
    }

    public Boolean getOneConsequencePerEvidence() {
        return oneConsequencePerEvidence;
    }

    public ClinicalInterpretationConfiguration setOneConsequencePerEvidence(Boolean oneConsequencePerEvidence) {
        this.oneConsequencePerEvidence = oneConsequencePerEvidence;
        return this;
    }

    public TierConfiguration getTierConfiguration() {
        return tierConfiguration;
    }

    public ClinicalInterpretationConfiguration setTierConfiguration(TierConfiguration tierConfiguration) {
        this.tierConfiguration = tierConfiguration;
        return this;
    }
}
