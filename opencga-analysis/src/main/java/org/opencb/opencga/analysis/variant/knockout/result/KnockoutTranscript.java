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

package org.opencb.opencga.analysis.variant.knockout.result;

import java.util.LinkedList;
import java.util.List;

public class KnockoutTranscript {

    private String id;
    private String biotype;
    private List<KnockoutVariant> variants = new LinkedList<>();

    public KnockoutTranscript() {
    }

    public KnockoutTranscript(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public KnockoutTranscript setId(String id) {
        this.id = id;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public KnockoutTranscript setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public List<KnockoutVariant> getVariants() {
        return variants;
    }

    public KnockoutTranscript setVariants(List<KnockoutVariant> variants) {
        this.variants = variants;
        return this;
    }

    public KnockoutTranscript addVariant(KnockoutVariant variant) {
        this.variants.add(variant);
        return this;
    }
}
