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

package org.opencb.opencga.core.models.individual;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.AbstractAclEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class IndividualAclEntry extends AbstractAclEntry<IndividualAclEntry.IndividualPermissions> {

    public enum IndividualPermissions {
        VIEW(Collections.emptyList()),
        WRITE(Collections.singletonList(VIEW)),
        DELETE(Arrays.asList(VIEW, WRITE)),
        VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
        WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
        DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW));

        private List<IndividualPermissions> implicitPermissions;

        IndividualPermissions(List<IndividualPermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        public List<IndividualPermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<IndividualPermissions> getDependentPermissions() {
            List<IndividualPermissions> dependentPermissions = new LinkedList<>();
            for (IndividualPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }

    public IndividualAclEntry() {
        this("", Collections.emptyList());
    }

    public IndividualAclEntry(String member, EnumSet<IndividualPermissions> permissions) {
        super(member, permissions);
    }

    public IndividualAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(IndividualPermissions.class));

        EnumSet<IndividualPermissions> aux = EnumSet.allOf(IndividualPermissions.class);
        for (IndividualPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public IndividualAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(IndividualPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(IndividualPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}
