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

package org.opencb.opencga.core.models.monitor;

import java.util.Collections;
import java.util.List;

public class HealthCheckDependencies {
    private List<HealthCheckDependency> datastores;
    private List<HealthCheckDependency> apis;

    public HealthCheckDependencies() {
        this(Collections.emptyList(), Collections.emptyList());
    }

    public HealthCheckDependencies(List<HealthCheckDependency> datastores, List<HealthCheckDependency> apis) {
        this.datastores = datastores;
        this.apis = apis;
    }

    public List<HealthCheckDependency> getDatastores() {
        return datastores;
    }

    public HealthCheckDependencies setDatastores(List<HealthCheckDependency> datastores) {
        this.datastores = datastores;
        return this;
    }

    public List<HealthCheckDependency> getApis() {
        return apis;
    }

    public HealthCheckDependencies setApis(List<HealthCheckDependency> apis) {
        this.apis = apis;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HealthCheckDependencies{");
        sb.append("datastores=").append(datastores);
        sb.append(", apis=").append(apis);
        sb.append('}');
        return sb.toString();
    }
}
