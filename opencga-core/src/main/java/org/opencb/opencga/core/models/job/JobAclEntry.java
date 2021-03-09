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

package org.opencb.opencga.core.models.job;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.AbstractAclEntry;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class JobAclEntry extends AbstractAclEntry<JobAclEntry.JobPermissions> {

    public enum JobPermissions {
        VIEW(Collections.emptyList()),
        WRITE(Collections.singletonList(VIEW)),
        DELETE(Arrays.asList(VIEW, WRITE));

        private List<JobPermissions> implicitPermissions;

        JobPermissions(List<JobPermissions> implicitPermissions) {
            this.implicitPermissions = implicitPermissions;
        }

        public List<JobPermissions> getImplicitPermissions() {
            return implicitPermissions;
        }

        public List<JobPermissions> getDependentPermissions() {
            List<JobPermissions> dependentPermissions = new LinkedList<>();
            for (JobPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
                if (permission.getImplicitPermissions().contains(this)) {
                    dependentPermissions.add(permission);
                }
            }
            return dependentPermissions;
        }
    }

    public JobAclEntry() {
        this("", Collections.emptyList());
    }

    public JobAclEntry(String member, EnumSet<JobPermissions> permissions) {
        super(member, permissions);
    }

    public JobAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(JobPermissions.class));

        EnumSet<JobPermissions> aux = EnumSet.allOf(JobPermissions.class);
        for (JobPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public JobAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(JobPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(JobPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}
