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

import java.util.Map;

/**
 * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh interdum
 * finibus.
 */
public class SampleCollection {
    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     */
    private String tissue;

    /**
     * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh
     * interdum finibus.
     *
     * @apiNote Internal, Unique, Immutable
     * @implNote The sample collection is a list of samples
     * @see [ZetaGenomics] (https://www.zettagenomics.com)
     * @since 2.1
     */
    private String organ;
    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     * @deprecated
     */
    private String quantity;
    /**
     * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh
     * interdum finibus.
     *
     * @apiNote Required, Immutable
     */
    private String method;
    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     */
    private String date;
    /**
     * Proin aliquam ante in ligula tincidunt, cursus volutpat urna suscipit. Phasellus interdum, libero at posuere blandit, felis dui
     * dignissim leo, quis ullamcorper felis elit a augue.
     *
     * @apiNote Required
     */
    private Map<String, Object> attributes;

    public SampleCollection() {
    }

    public SampleCollection(String tissue, String organ, String quantity, String method, String date, Map<String, Object> attributes) {
        this.tissue = tissue;
        this.organ = organ;
        this.quantity = quantity;
        this.method = method;
        this.date = date;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleCollection{");
        sb.append("tissue='").append(tissue).append('\'');
        sb.append(", organ='").append(organ).append('\'');
        sb.append(", quantity='").append(quantity).append('\'');
        sb.append(", method='").append(method).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", attributes='").append(attributes).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getTissue() {
        return tissue;
    }

    public SampleCollection setTissue(String tissue) {
        this.tissue = tissue;
        return this;
    }

    public String getOrgan() {
        return organ;
    }

    public SampleCollection setOrgan(String organ) {
        this.organ = organ;
        return this;
    }

    public String getQuantity() {
        return quantity;
    }

    public SampleCollection setQuantity(String quantity) {
        this.quantity = quantity;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public SampleCollection setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getDate() {
        return date;
    }

    public SampleCollection setDate(String date) {
        this.date = date;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public SampleCollection setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
