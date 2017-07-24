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

package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.models.acls.AbstractAcl;
import org.opencb.opencga.catalog.models.acls.permissions.ClinicalAnalysisAclEntry;

import java.util.Map;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysis extends AbstractAcl<ClinicalAnalysisAclEntry> {

    private long id;
    private String name;
    private String description;
    private Type type;

    private Family family;
    private Individual proband;
    private Sample sample;

    private String creationDate;
    private Status status;
    private int release;
    private Map<String, Object> attributes;

    public enum Type {
        SINGLE, DUO, TRIO, FAMILY, AUTO, MULTISAMPLE
    }

    public ClinicalAnalysis() {
    }

    public ClinicalAnalysis(long id, String name, String description, Type type, Family family, Individual proband, Sample sample,
                            String creationDate, Status status, int release, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;
        this.family = family;
        this.proband = proband;
        this.sample = sample;
        this.creationDate = creationDate;
        this.status = status;
        this.release = release;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysis{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", family=").append(family);
        sb.append(", proband=").append(proband);
        sb.append(", sample=").append(sample);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append(", acl=").append(acl);
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

    public Sample getSample() {
        return sample;
    }

    public ClinicalAnalysis setSample(Sample sample) {
        this.sample = sample;
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
}
