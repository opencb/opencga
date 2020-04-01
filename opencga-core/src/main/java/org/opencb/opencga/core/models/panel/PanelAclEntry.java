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

package org.opencb.opencga.core.models.panel;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.AbstractAclEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 01/06/16.
 */
public class PanelAclEntry extends AbstractAclEntry<PanelAclEntry.PanelPermissions> {

    public enum PanelPermissions {
        VIEW(Collections.emptyList()),
        UPDATE(Collections.singletonList(VIEW)),
        DELETE(Arrays.asList(VIEW, UPDATE));

        private List<PanelPermissions> implicitPermissions;

        PanelPermissions(List<PanelPermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        public List<PanelPermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<PanelPermissions> getDependentPermissions() {
            List<PanelPermissions> dependentPermissions = new LinkedList<>();
            for (PanelPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }

    public PanelAclEntry() {
        this("", Collections.emptyList());
    }

    public PanelAclEntry(String member, EnumSet<PanelPermissions> permissions) {
        super(member, permissions);
    }

    public PanelAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(PanelPermissions.class));

        EnumSet<PanelPermissions> aux = EnumSet.allOf(PanelPermissions.class);
        for (PanelPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public PanelAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(PanelPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(PanelPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}
