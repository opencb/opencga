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

package org.opencb.opencga.client.config;

/**
 * Created by imedina on 04/05/17.
 */
public class VariantClientConfiguration {

    private String unknownGenotype;

    public VariantClientConfiguration() {
    }

    public VariantClientConfiguration(String unknownGenotype) {
        this.unknownGenotype = unknownGenotype;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantClientConfiguration{");
        sb.append("unknownGenotype='").append(unknownGenotype);
        return sb.toString();
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public VariantClientConfiguration setUnknownGenotype(String unknownGenotype) {
        this.unknownGenotype = unknownGenotype;
        return this;
    }

}
