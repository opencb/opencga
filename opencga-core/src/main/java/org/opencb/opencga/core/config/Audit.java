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

package org.opencb.opencga.core.config;

import java.util.List;

/**
 * Created by pfurio on 02/06/16.
 */
public class Audit {

    private long maxDocuments;
    private long maxSize;
    private String javaClass;
    private List<String> exclude;

    public Audit() {
    }

    public Audit(long maxDocuments, long maxSize, String javaClass, List<String> exclude) {
        this.maxDocuments = maxDocuments;
        this.maxSize = maxSize;
        this.javaClass = javaClass;
        this.exclude = exclude;
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

    public String getJavaClass() {
        return javaClass;
    }

    public Audit setJavaClass(String javaClass) {
        this.javaClass = javaClass;
        return this;
    }

    public List<String> getExclude() {
        return exclude;
    }

    public Audit setExclude(List<String> exclude) {
        this.exclude = exclude;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Audit{");
        sb.append("maxDocuments=").append(maxDocuments);
        sb.append(", maxSize=").append(maxSize);
        sb.append(", javaClass='").append(javaClass).append('\'');
        sb.append(", exclude=").append(exclude);
        sb.append('}');
        return sb.toString();
    }
}
