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

package org.opencb.opencga.core.models;

abstract public class PrivateStudyUid extends PrivateFields implements IPrivateStudyUid {

    private String id;
    private long studyUid;

    public PrivateStudyUid() {
    }

    public PrivateStudyUid(long studyUid) {
        this.studyUid = studyUid;
    }

    @Override
    public long getStudyUid() {
        return studyUid;
    }

    @Override
    public PrivateStudyUid setStudyUid(long studyUid) {
        this.studyUid = studyUid;
        return this;
    }

    public String getId() {
        return id;
    }

    public PrivateStudyUid setId(String id) {
        this.id = id;
        return this;
    }
}
