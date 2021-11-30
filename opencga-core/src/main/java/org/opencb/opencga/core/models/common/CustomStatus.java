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

package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.annotations.DataModel;

/**
 * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh interdum
 * finibus.
 */
public class CustomStatus {

    /**
     * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh
     * interdum finibus.
     *
     * @apiNote Required, Immutable
     */
    @DataModel(id = "CustomStatus.name", name = "name", indexed = true,
            description = FieldConstants.CUSTOM_STATUS_NAME_DESCRIPTION)
    private String name;
    /**
     * Proin aliquam ante in ligula tincidunt, cursus volutpat urna suscipit. Phasellus interdum, libero at posuere blandit, felis dui
     * dignissim leo, quis ullamcorper felis elit a augue.
     *
     * @apiNote Required
     */
    @DataModel(id = "CustomStatus.description", name = "description", indexed = true,
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    /**
     * Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus
     * lectus, ut ultrices nunc vulputate ac.
     *
     * @apiNote Internal, Unique, Immutable
     */
    @DataModel(id = "CustomStatus.date", name = "date", managed = true,
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String date;

    public CustomStatus() {
        this("", "", "");
    }

    public CustomStatus(String name, String description, String date) {
        this.name = name;
        this.description = description;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomStatus{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public CustomStatus setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CustomStatus setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getDate() {
        return date;
    }

    public CustomStatus setDate(String date) {
        this.date = date;
        return this;
    }
}
