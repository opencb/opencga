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

import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantAnnotationIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant annotation index params.";

    public VariantAnnotationIndexParams() {
    }

    public VariantAnnotationIndexParams(String outdir, String outputFileName, String annotator,
                                        boolean overwriteAnnotations, String region, boolean create, String load, String customName,
                                        YesNoAuto sampleIndexAnnotation) {
        this.outdir = outdir;
        this.outputFileName = outputFileName;
        this.annotator = annotator;
        this.overwriteAnnotations = overwriteAnnotations;
        this.region = region;
        this.create = create;
        this.load = load;
        this.customName = customName;
        this.sampleIndexAnnotation = sampleIndexAnnotation;
    }

    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_OUTPUT_FILE_NAME_DESCRIPTION)
    private String outputFileName;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_ANNOTATOR_DESCRIPTION)
    private String annotator;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_OVERWRITE_ANNOTATIONS_DESCRIPTION)
    private boolean overwriteAnnotations;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_REGION_DESCRIPTION)
    private String region;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_CREATE_DESCRIPTION)
    private boolean create;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_LOAD_DESCRIPTION)
    private String load;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_CUSTOM_NAME_DESCRIPTION)
    private String customName;
    @DataField(description = ParamConstants.VARIANT_ANNOTATION_INDEX_PARAMS_SAMPLE_INDEX_ANNOTATION_DESCRIPTION)
    private YesNoAuto sampleIndexAnnotation;

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

    public YesNoAuto getSampleIndexAnnotation() {
        return sampleIndexAnnotation;
    }

    public VariantAnnotationIndexParams setSampleIndexAnnotation(YesNoAuto sampleIndexAnnotation) {
        this.sampleIndexAnnotation = sampleIndexAnnotation;
        return this;
    }
}
