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

package org.opencb.opencga.client.config;

import java.util.Map;

/**
 * Created by imedina on 04/05/17.
 */
public class VariantClientConfiguration {

    private String unknownGenotype;
    private Map<String, String> includeFormats;

    public VariantClientConfiguration() {
    }

    public VariantClientConfiguration(String unknownGenotype, Map<String, String> includeFormats) {
        this.unknownGenotype = unknownGenotype;
        this.includeFormats = includeFormats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantClientConfiguration{");
        sb.append("unknownGenotype='").append(unknownGenotype).append('\'');
        sb.append(", includeFormats=").append(includeFormats);
        sb.append('}');
        return sb.toString();
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public VariantClientConfiguration setUnknownGenotype(String unknownGenotype) {
        this.unknownGenotype = unknownGenotype;
        return this;
    }

    public Map<String, String> getIncludeFormats() {
        return includeFormats;
    }

    public VariantClientConfiguration setIncludeFormats(Map<String, String> includeFormats) {
        this.includeFormats = includeFormats;
        return this;
    }
}
