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

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.Status;

public class ProjectInternal extends Internal {

    private Datastores datastores;
    private CellBaseConfiguration cellbase;

    public ProjectInternal() {
    }

    public ProjectInternal(Status status, String registrationDate, String modificationDate, Datastores datastores,
                           CellBaseConfiguration cellbase) {
        super(status, registrationDate, modificationDate);
        this.datastores = datastores;
        this.cellbase = cellbase;
    }

    public static ProjectInternal init() {
        return new ProjectInternal(new Status(), TimeUtils.getTime(), TimeUtils.getTime(), new Datastores(), new CellBaseConfiguration());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", datastores=").append(datastores);
        sb.append(", cellbase=").append(cellbase);
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

    public CellBaseConfiguration getCellbase() {
        return cellbase;
    }

    public ProjectInternal setCellbase(CellBaseConfiguration cellbase) {
        this.cellbase = cellbase;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public ProjectInternal setStatus(Status status) {
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

    public String getModificationDate() {
        return modificationDate;
    }

    public ProjectInternal setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }
}
