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

package org.opencb.opencga.core.models.sample;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.AbstractAclEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class SampleAclEntry extends AbstractAclEntry<SampleAclEntry.SamplePermissions> {

    public enum SamplePermissions {
        VIEW(Collections.emptyList()),
        WRITE(Collections.singletonList(VIEW)),
        DELETE(Arrays.asList(VIEW, WRITE)),
        VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
        WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
        DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW)),
        VIEW_VARIANTS(Arrays.asList(VIEW, VIEW_ANNOTATIONS));

        private List<SamplePermissions> implicitPermissions;

        SamplePermissions(List<SamplePermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        public List<SamplePermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<SamplePermissions> getDependentPermissions() {
            List<SamplePermissions> dependentPermissions = new LinkedList<>();
            for (SamplePermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }

    public SampleAclEntry() {
        this("", Collections.emptyList());
    }

    public SampleAclEntry(String member, EnumSet<SamplePermissions> permissions) {
        super(member, permissions);
    }

    public SampleAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(SamplePermissions.class));

        EnumSet<SamplePermissions> aux = EnumSet.allOf(SamplePermissions.class);
        for (SamplePermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public SampleAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(SamplePermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(SamplePermissions::valueOf).collect(Collectors.toList()));
        }
    }

}
