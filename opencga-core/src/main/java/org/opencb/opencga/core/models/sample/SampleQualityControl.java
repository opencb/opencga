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

package org.opencb.opencga.core.models.sample;

import org.opencb.biodata.models.clinical.ClinicalComment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh interdum
 * finibus.
 */
public class SampleQualityControl implements Serializable {
    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     */
    private List<String> files;
    /**
     * Proin aliquam ante in ligula tincidunt, cursus volutpat urna suscipit. Phasellus interdum, libero at posuere blandit, felis dui
     * dignissim leo, quis ullamcorper felis elit a augue.
     *
     * @apiNote Required
     */
    private List<ClinicalComment> comments;
    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     */
    private SampleVariantQualityControlMetrics variantMetrics;

    public SampleQualityControl() {
        this(new ArrayList<>(), new ArrayList<>(), new SampleVariantQualityControlMetrics());
    }

    public SampleQualityControl(List<String> files, List<ClinicalComment> comments, SampleVariantQualityControlMetrics variantMetrics) {
        this.files = files;
        this.comments = comments;
        this.variantMetrics = variantMetrics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQualityControl{");
        sb.append("files=").append(files);
        sb.append(", comments=").append(comments);
        sb.append(", variantMetrics=").append(variantMetrics);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getFiles() {
        return files;
    }

    public SampleQualityControl setFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public SampleQualityControl setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }

    public SampleVariantQualityControlMetrics getVariantMetrics() {
        return variantMetrics;
    }

    public SampleQualityControl setVariantMetrics(SampleVariantQualityControlMetrics variantMetrics) {
        this.variantMetrics = variantMetrics;
        return this;
    }
}
