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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.core.models.AbstractAclEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 02/05/17.
 */
public class FamilyAclEntry extends AbstractAclEntry<FamilyAclEntry.FamilyPermissions> {

    public enum FamilyPermissions {
        VIEW(Collections.emptyList()),
        UPDATE(Collections.singletonList(VIEW)),
        DELETE(Arrays.asList(VIEW, UPDATE)),
        VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
        WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
        DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW));

        private List<FamilyPermissions> implicitPermissions;

        FamilyPermissions(List<FamilyPermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        public List<FamilyPermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<FamilyPermissions> getDependentPermissions() {
            List<FamilyPermissions> dependentPermissions = new LinkedList<>();
            for (FamilyPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }

    public FamilyAclEntry() {
        this("", Collections.emptyList());
    }

    public FamilyAclEntry(String member, EnumSet<FamilyPermissions> permissions) {
        super(member, permissions);
    }

    public FamilyAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(FamilyPermissions.class));

        EnumSet<FamilyPermissions> aux = EnumSet.allOf(FamilyPermissions.class);
        for (FamilyPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public FamilyAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(FamilyPermissions.class));
        if (CollectionUtils.isNotEmpty(permissions)) {
            this.permissions.addAll(permissions.stream().map(FamilyPermissions::valueOf).collect(Collectors.toList()));
        }
    }
}
