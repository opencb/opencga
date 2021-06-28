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

import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.common.Status;

public class ProjectInternal {

    private Datastores datastores;
    private CellBaseConfiguration cellbase;
    private Status status;

    public ProjectInternal() {
    }

    public ProjectInternal(Datastores datastores, Status status) {
        this.datastores = datastores;
        this.status = status;
    }

    public ProjectInternal(Datastores datastores, CellBaseConfiguration cellbase, Status status) {
        this.datastores = datastores;
        this.cellbase = cellbase;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternal{");
        sb.append("datastores=").append(datastores);
        sb.append(", status=").append(status);
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
}
