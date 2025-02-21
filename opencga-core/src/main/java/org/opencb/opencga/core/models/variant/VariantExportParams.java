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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.opencb.commons.datastore.core.Query;

public class VariantExportParams extends VariantQueryParams {
    public static final String DESCRIPTION = "Variant export params";

    private String outputFileName;
    private String outputFileFormat;
    private String variantsFile;
    private String include;
    private String exclude;
    private Integer limit;
    private Integer skip;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean summary;

    public VariantExportParams() {
    }

    public VariantExportParams(Query query, String outputFileName, String outputFileFormat,
                               String variantsFile) {
        super(query);
        this.outputFileName = outputFileName;
        this.outputFileFormat = outputFileFormat;
        this.variantsFile = variantsFile;
    }

    @Deprecated
    @JsonIgnore
    public String getOutdir() {
        return null;
    }

    @Deprecated
    @JsonIgnore
    public VariantExportParams setOutdir(String unused) {
        return this;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public VariantExportParams setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }

    public String getOutputFileFormat() {
        return outputFileFormat;
    }

    public VariantExportParams setOutputFileFormat(String outputFileFormat) {
        this.outputFileFormat = outputFileFormat;
        return this;
    }

    public VariantExportParams setVariantsFile(String variantsFile) {

        this.variantsFile = variantsFile;
        return this;
    }

    public String getVariantsFile() {
        return variantsFile;
    }

    public String getInclude() {
        return include;
    }

    public VariantExportParams setInclude(String include) {
        this.include = include;
        return this;
    }

    public String getExclude() {
        return exclude;
    }

    public VariantExportParams setExclude(String exclude) {
        this.exclude = exclude;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public VariantExportParams setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getSkip() {
        return skip;
    }

    public VariantExportParams setSkip(Integer skip) {
        this.skip = skip;
        return this;
    }

    public boolean isSummary() {
        return summary;
    }

    public VariantExportParams setSummary(boolean summary) {
        this.summary = summary;
        return this;
    }
}
