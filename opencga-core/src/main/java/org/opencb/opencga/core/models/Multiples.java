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

package org.opencb.opencga.core.models;

import java.util.List;

public class Multiples {

    private String type;
    private List<String> siblings;


    public Multiples() {
    }

    public Multiples(String type, List<String> siblings) {
        this.type = type;
        this.siblings = siblings;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Multiples{");
        sb.append("type='").append(type).append('\'');
        sb.append(", siblings=").append(siblings);
        sb.append('}');
        return sb.toString();
    }


    public String getType() {
        return type;
    }

    public Multiples setType(String type) {
        this.type = type;
        return this;
    }

    public List<String> getSiblings() {
        return siblings;
    }

    public Multiples setSiblings(List<String> siblings) {
        this.siblings = siblings;
        return this;
    }
}
