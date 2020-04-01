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

package org.opencb.opencga.core.config;

/**
 * Created by pfurio on 02/06/16.
 */
public class Audit {

    private String manager;
    private long maxDocuments;
    private long maxSize;

    public Audit() {
    }

    public Audit(String manager, long maxDocuments, long maxSize) {
        this.manager = manager;
        this.maxDocuments = maxDocuments;
        this.maxSize = maxSize;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Audit{");
        sb.append("manager='").append(manager).append('\'');
        sb.append(", maxDocuments=").append(maxDocuments);
        sb.append(", maxSize=").append(maxSize);
        sb.append('}');
        return sb.toString();
    }

    public String getManager() {
        return manager;
    }

    public Audit setManager(String manager) {
        this.manager = manager;
        return this;
    }

    public long getMaxDocuments() {
        return maxDocuments;
    }

    public Audit setMaxDocuments(long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public Audit setMaxSize(long maxSize) {
        this.maxSize = maxSize;
        return this;
    }
}
