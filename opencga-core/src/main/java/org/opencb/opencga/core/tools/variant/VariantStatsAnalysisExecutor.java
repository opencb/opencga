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

package org.opencb.opencga.core.tools.variant;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public abstract class VariantStatsAnalysisExecutor extends OpenCgaToolExecutor {

    private Path outputFile;
    private String study;
    private Map<String, List<String>> cohorts;
    private Query variantsQuery;

    public VariantStatsAnalysisExecutor() {
    }

    public String getStudy() {
        return study;
    }

    public VariantStatsAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Map<String, List<String>> getCohorts() {
        return cohorts;
    }

    public VariantStatsAnalysisExecutor setCohorts(Map<String, List<String>> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public VariantStatsAnalysisExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public VariantStatsAnalysisExecutor setVariantsQuery(Query variantsQuery) {
        this.variantsQuery = variantsQuery;
        return this;
    }

    public Query getVariantsQuery() {
        return variantsQuery;
    }

}
