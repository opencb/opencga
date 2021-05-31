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

package org.opencb.opencga.core.models.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.ToolParams;

public class MutationalSignatureAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Mutational signature analysis params";

    private String sample;
    private String id;
    private String description;
    private SampleQcSignatureQueryParams query;
    private boolean fitting;

    private String outdir;

    public MutationalSignatureAnalysisParams() {
    }

    public MutationalSignatureAnalysisParams(String sample, String id, String description, SampleQcSignatureQueryParams query,
                                             boolean fitting, String outdir) {
        this.sample = sample;
        this.id = id;
        this.description = description;
        this.query = query;
        this.fitting = fitting;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MutationalSignatureAnalysisParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", query=").append(query);
        sb.append(", fitting=").append(fitting);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public MutationalSignatureAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getId() {
        return id;
    }

    public MutationalSignatureAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MutationalSignatureAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public SampleQcSignatureQueryParams getQuery() {
        return query;
    }

    public MutationalSignatureAnalysisParams setQuery(SampleQcSignatureQueryParams query) {
        this.query = query;
        return this;
    }

    public boolean isFitting() {
        return fitting;
    }

    public MutationalSignatureAnalysisParams setFitting(boolean fitting) {
        this.fitting = fitting;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public MutationalSignatureAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
