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

import org.opencb.opencga.core.models.file.File;

import java.util.Objects;

public class Datastores {

    private DataStore variant;
//    private DataStore alignment;
//    private DataStore expression;

    public Datastores() {
    }

    public Datastores(DataStore variant) {
        this.variant = variant;
    }

    public DataStore getDataStore(File.Bioformat bioformat) {
        switch (bioformat) {
            case VARIANT:
                return variant;
            default:
                return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Datastores that = (Datastores) o;
        return Objects.equals(variant, that.variant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variant);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataStores{");
        sb.append("variant=").append(variant);
        sb.append('}');
        return sb.toString();
    }

    public DataStore getVariant() {
        return variant;
    }

    public Datastores setVariant(DataStore variant) {
        this.variant = variant;
        return this;
    }

}
