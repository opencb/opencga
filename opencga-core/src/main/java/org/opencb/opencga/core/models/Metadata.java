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

import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;

/**
 * Created by imedina on 13/09/14.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class Metadata {

    @DataField(description = ParamConstants.METADATA_VERSION_DESCRIPTION)
    private String version;
    @DataField(description = ParamConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(description = ParamConstants.METADATA_ID_COUNTER_DESCRIPTION)
    private long idCounter;


    public Metadata() {
        this(GitRepositoryState.get().getBuildVersion(), TimeUtils.getTime());
    }

    public Metadata(String version, String creationDate) {
        this(version, creationDate, 0);
    }

    public Metadata(String version, String creationDate, long idCounter) {
        this.version = version;
        this.creationDate = creationDate;
        this.idCounter = idCounter;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Metadata{");
        sb.append("version='").append(version).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", idCounter=").append(idCounter);
        sb.append('}');
        return sb.toString();
    }

    public String getVersion() {
        return version;
    }

    public Metadata setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Metadata setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public long getIdCounter() {
        return idCounter;
    }

    public Metadata setIdCounter(long idCounter) {
        this.idCounter = idCounter;
        return this;
    }
}
