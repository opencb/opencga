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

package org.opencb.opencga.core.models;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 24/11/14.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

@Deprecated
public class Dataset {

    @DataField(description = ParamConstants.DATASET_ID_DESCRIPTION)
    private long id;
    @DataField(description = ParamConstants.DATASET_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(description = ParamConstants.DATASET_FILES_DESCRIPTION)
    private List<Long> files;
    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private InternalStatus status;

    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;


    public Dataset() {
    }

    public Dataset(int id, String name, String creationDate, String description, List<Long> files, InternalStatus status,
                   Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.creationDate = creationDate;
        this.description = description;
        this.files = files;
        this.status = status;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Dataset{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", files=").append(files);
        sb.append(", status=").append(status);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Dataset setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Dataset setName(String name) {
        this.name = name;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Dataset setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Dataset setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Long> getFiles() {
        return files;
    }

    public Dataset setFiles(List<Long> files) {
        this.files = files;
        return this;
    }

    public InternalStatus getStatus() {
        return status;
    }

    public Dataset setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Dataset setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
