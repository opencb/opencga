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

package org.opencb.opencga.core.models.family;

import org.opencb.opencga.core.models.AclParams;

public class FamilyAclUpdateParams extends AclParams {

    private String family;

    public FamilyAclUpdateParams() {
    }

    public FamilyAclUpdateParams(String permissions, String family) {
        super(permissions);
        this.family = family;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyAclUpdateParams{");
        sb.append("family='").append(family).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFamily() {
        return family;
    }

    public FamilyAclUpdateParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public FamilyAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
