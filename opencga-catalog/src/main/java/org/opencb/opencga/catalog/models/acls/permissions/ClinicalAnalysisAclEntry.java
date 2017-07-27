/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.models.acls.permissions;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisAclEntry extends AbstractAclEntry<ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions> {

    public enum ClinicalAnalysisPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public ClinicalAnalysisAclEntry() {
        this("", Collections.emptyList());
    }

    public ClinicalAnalysisAclEntry(String member, EnumSet<ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions> permissions) {
        super(member, permissions);
    }

    public ClinicalAnalysisAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.class));

        EnumSet<ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions> aux =
                EnumSet.allOf(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.class);
        for (ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public ClinicalAnalysisAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.class));

        if (permissions.size() > 0) {
            this.permissions.addAll(
                    permissions.stream().map(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::valueOf).collect(Collectors.toList()));
        }
    }
}
