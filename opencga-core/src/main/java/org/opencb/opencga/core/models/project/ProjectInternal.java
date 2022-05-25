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

package org.opencb.opencga.core.models.project;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;

public class ProjectInternal extends Internal {

    @DataField(id = "datastores", indexed = true, uncommentedClasses = {"Datastores"},
            description = FieldConstants.PROJECT_INTERNAL_DATA_STORES)
    private Datastores datastores;


    public ProjectInternal() {
    }

    public ProjectInternal(InternalStatus status, String registrationDate, String modificationDate, Datastores datastores) {
        super(status, registrationDate, modificationDate);
        this.datastores = datastores;
    }

    public static ProjectInternal init() {
        return new ProjectInternal(new InternalStatus(), TimeUtils.getTime(), TimeUtils.getTime(), new Datastores());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(lastModified).append('\'');
        sb.append(", datastores=").append(datastores);
        sb.append('}');
        return sb.toString();
    }

    public Datastores getDatastores() {
        return datastores;
    }

    public ProjectInternal setDatastores(Datastores datastores) {
        this.datastores = datastores;
        return this;
    }


    public InternalStatus getStatus() {
        return status;
    }

    public ProjectInternal setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public ProjectInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public ProjectInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
