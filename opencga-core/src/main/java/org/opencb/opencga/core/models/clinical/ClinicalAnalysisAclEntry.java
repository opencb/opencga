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

package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.core.models.AbstractAclEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisAclEntry extends AbstractAclEntry<ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions> {

    public enum ClinicalAnalysisPermissions {
        VIEW(Collections.emptyList()),
        WRITE(Collections.singletonList(VIEW)),
        DELETE(Arrays.asList(VIEW, WRITE));

        private List<ClinicalAnalysisPermissions> implicitPermissions;

        ClinicalAnalysisPermissions(List<ClinicalAnalysisPermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        public List<ClinicalAnalysisPermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<ClinicalAnalysisPermissions> getDependentPermissions() {
            List<ClinicalAnalysisPermissions> dependentPermissions = new LinkedList<>();
            for (ClinicalAnalysisPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }        
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

        if (CollectionUtils.isNotEmpty(permissions)) {
            this.permissions.addAll(
                    permissions.stream().map(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::valueOf).collect(Collectors.toList()));
        }
    }
}
