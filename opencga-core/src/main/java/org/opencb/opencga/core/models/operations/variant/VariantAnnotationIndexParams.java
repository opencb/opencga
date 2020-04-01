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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantAnnotationIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant annotation index params.";

    public VariantAnnotationIndexParams() {
    }

    public VariantAnnotationIndexParams(String outdir, String outputFileName, String annotator,
                                        boolean overwriteAnnotations, String region, boolean create, String load, String customName) {
        this.outdir = outdir;
        this.outputFileName = outputFileName;
        this.annotator = annotator;
        this.overwriteAnnotations = overwriteAnnotations;
        this.region = region;
        this.create = create;
        this.load = load;
        this.customName = customName;
    }

    private String outdir;
    private String outputFileName;
    private String annotator;
    private boolean overwriteAnnotations;
    private String region;
    private boolean create;
    private String load;
    private String customName;

    public String getOutdir() {
        return outdir;
    }

    public VariantAnnotationIndexParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public String getAnnotator() {
        return annotator;
    }

    public VariantAnnotationIndexParams setAnnotator(String annotator) {
        this.annotator = annotator;
        return this;
    }

    public boolean isOverwriteAnnotations() {
        return overwriteAnnotations;
    }

    public VariantAnnotationIndexParams setOverwriteAnnotations(boolean overwriteAnnotations) {
        this.overwriteAnnotations = overwriteAnnotations;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public VariantAnnotationIndexParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public boolean isCreate() {
        return create;
    }

    public VariantAnnotationIndexParams setCreate(boolean create) {
        this.create = create;
        return this;
    }

    public String getLoad() {
        return load;
    }

    public VariantAnnotationIndexParams setLoad(String load) {
        this.load = load;
        return this;
    }

    public String getCustomName() {
        return customName;
    }

    public VariantAnnotationIndexParams setCustomName(String customName) {
        this.customName = customName;
        return this;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public VariantAnnotationIndexParams setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }
}
