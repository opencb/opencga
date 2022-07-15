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

package org.opencb.opencga.core.models.cohort;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.AclEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class CohortAclEntry extends AclEntry<CohortAclEntry.CohortPermissions> {

    public enum CohortPermissions {
        VIEW(Collections.emptyList()),
        WRITE(Collections.singletonList(VIEW)),
        DELETE(Arrays.asList(VIEW, WRITE)),
        VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
        WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
        DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW));

        private List<CohortPermissions> implicitPermissions;

        CohortPermissions(List<CohortPermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        public List<CohortPermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<CohortPermissions> getDependentPermissions() {
            List<CohortPermissions> dependentPermissions = new LinkedList<>();
            for (CohortPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }

    public CohortAclEntry() {
        this("", Collections.emptyList());
    }

    public CohortAclEntry(String member, EnumSet<CohortPermissions> permissions) {
        super(member, permissions);
    }

    public CohortAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(CohortPermissions.class));

        EnumSet<CohortPermissions> aux = EnumSet.allOf(CohortPermissions.class);
        for (CohortPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public CohortAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(CohortPermissions.class));

        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(CohortPermissions::valueOf).collect(Collectors.toList()));
        }
    }
}
