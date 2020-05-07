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

package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.models.PrivateStudyUid;

import java.util.List;

/**
 * Created by pfurio on 07/07/16.
 */
public abstract class Annotable extends PrivateStudyUid {

    private String id;
    protected List<AnnotationSet> annotationSets;

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public Annotable setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public String getId() {
        return id;
    }

    public Annotable setId(String id) {
        this.id = id;
        return this;
    }
}
