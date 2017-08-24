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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysis {

    private long id;
    private String name;
    private String description;
    private Type type;

    private Individual proband;
    private Family family;
    private List<ClinicalInterpretation> interpretations;

    private String creationDate;
    private Status status;
    private int release;
    private Map<String, Object> attributes;

    public enum Type {
        SINGLE, DUO, TRIO, FAMILY, AUTO, MULTISAMPLE
    }

    // Todo: Think about a better place to have this enum
    public enum Action {
        ADD,
        SET,
        REMOVE
    }

    public ClinicalAnalysis() {
    }

    public ClinicalAnalysis(long id, String name, String description, Type type, Family family, Individual proband,
                            List <ClinicalInterpretation> interpretations, String creationDate, Status status, int release,
                            Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;
        this.family = family;
        this.proband = proband;
        this.interpretations = defaultObject(interpretations, ArrayList::new);
        this.creationDate = creationDate;
        this.status = status;
        this.release = release;
        this.attributes = defaultObject(attributes, HashMap::new);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysis{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", proband=").append(proband);
        sb.append(", family=").append(family);
        sb.append(", interpretations=").append(interpretations);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public ClinicalAnalysis setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalAnalysis setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysis setDescription(String description) {
        this.description = description;
        return this;
    }

    public Type getType() {
        return type;
    }

    public ClinicalAnalysis setType(Type type) {
        this.type = type;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public ClinicalAnalysis setFamily(Family family) {
        this.family = family;
        return this;
    }

    public Individual getProband() {
        return proband;
    }

    public ClinicalAnalysis setProband(Individual proband) {
        this.proband = proband;
        return this;
    }

    public List<ClinicalInterpretation> getInterpretations() {
        return interpretations;
    }

    public ClinicalAnalysis setInterpretations(List<ClinicalInterpretation> interpretations) {
        this.interpretations = interpretations;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ClinicalAnalysis setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public ClinicalAnalysis setStatus(Status status) {
        this.status = status;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public ClinicalAnalysis setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysis setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public static <O> O defaultObject(O object, Supplier<O> supplier) {
        if (object == null) {
            object = supplier.get();
        }
        return object;
    }

    public class ClinicalInterpretation {

        private String id;
        private String name;
        private long jobId;

        public ClinicalInterpretation() {
        }

        public ClinicalInterpretation(String id, String name, long jobId) {
            this.id = id;
            this.name = name;
            this.jobId = jobId;
        }


        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Interpretation{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", jobId=").append(jobId);
            sb.append('}');
            return sb.toString();
        }


        public String getId() {
            return id;
        }

        public ClinicalInterpretation setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public ClinicalInterpretation setName(String name) {
            this.name = name;
            return this;
        }

        public long getJobId() {
            return jobId;
        }

        public ClinicalInterpretation setJobId(long jobId) {
            this.jobId = jobId;
            return this;
        }
    }
}
