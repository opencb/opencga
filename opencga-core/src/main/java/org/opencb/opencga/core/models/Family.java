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

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by pfurio on 02/05/17.
 */
public class Family extends Annotable {

    private long id;
    private String name;

    private List<OntologyTerm> phenotypes;
    private List<Individual> members;

    private String creationDate;
    private FamilyStatus status;
    private String description;

    private int release;
    private int version;
    private Map<String, Object> attributes;

    public Family() {
    }

    public Family(String name, List<OntologyTerm> phenotypes, List<Individual> members, String description,
                  List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this(name, phenotypes, members, TimeUtils.getTime(), new FamilyStatus(Status.READY), description, -1, 1, annotationSets, attributes);
    }

    public Family(String name, List<OntologyTerm> phenotypes, List<Individual> members, String creationDate, FamilyStatus status,
                  String description, int release, int version, List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.name = name;
        this.phenotypes = defaultObject(phenotypes, Collections::emptyList);
        this.members = defaultObject(members, Collections::emptyList);
        this.creationDate = defaultObject(creationDate, TimeUtils::getTime);
        this.status = defaultObject(status, new FamilyStatus());
        this.description = description;
        this.release = release;
        this.version = version;
        this.annotationSets = defaultObject(annotationSets, Collections::emptyList);
        this.attributes = defaultObject(attributes, Collections::emptyMap);
    }

    public static class FamilyStatus extends Status {

        public static final String INCOMPLETE = "INCOMPLETE";

        public FamilyStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public FamilyStatus(String status) {
            this(status, "");
        }

        public FamilyStatus() {
            this(READY, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && (status.equals(INCOMPLETE))) {
                return true;
            }
            return false;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Family{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", members=").append(members);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", description='").append(description).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Family setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Family setName(String name) {
        this.name = name;
        return this;
    }

    public List<OntologyTerm> getPhenotypes() {
        return phenotypes;
    }

    public Family setPhenotypes(List<OntologyTerm> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Individual> getMembers() {
        return members;
    }

    public Family setMembers(List<Individual> members) {
        this.members = members;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Family setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public FamilyStatus getStatus() {
        return status;
    }

    public Family setStatus(FamilyStatus status) {
        this.status = status;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Family setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Family setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Family setVersion(int version) {
        this.version = version;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Family setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public static <O> O defaultObject(O object, O defaultObject) {
        if (object == null) {
            object = defaultObject;
        }
        return object;
    }

    public static <O> O defaultObject(O object, Supplier<O> supplier) {
        if (object == null) {
            object = supplier.get();
        }
        return object;
    }
}
