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

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Panel {

    private String id;
    private String name;
    private int version;

    private String author;
    private String status;
    private Date date;
    private String description;

    private List<OntologyTerm> phenotypes;

    private List<String> variants;
    private List<String> genes;
    private List<String> regions;

    private Map<String, Object> attributes;

    public Panel() {
    }

    public Panel(String id, String name, int version, String author, String status, Date date, String description, List<OntologyTerm>
            phenotypes, List<String> variants, List<String> genes, List<String> regions, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.author = author;
        this.status = status;
        this.date = date;
        this.description = description;
        this.phenotypes = phenotypes;
        this.variants = variants;
        this.genes = genes;
        this.regions = regions;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Panel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", version=").append(version);
        sb.append(", author='").append(author).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", date=").append(date);
        sb.append(", description='").append(description).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", variants=").append(variants);
        sb.append(", genes=").append(genes);
        sb.append(", regions=").append(regions);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Panel setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Panel setName(String name) {
        this.name = name;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Panel setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getAuthor() {
        return author;
    }

    public Panel setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public Panel setStatus(String status) {
        this.status = status;
        return this;
    }

    public Date getDate() {
        return date;
    }

    public Panel setDate(Date date) {
        this.date = date;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Panel setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<OntologyTerm> getPhenotypes() {
        return phenotypes;
    }

    public Panel setPhenotypes(List<OntologyTerm> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getVariants() {
        return variants;
    }

    public Panel setVariants(List<String> variants) {
        this.variants = variants;
        return this;
    }

    public List<String> getGenes() {
        return genes;
    }

    public Panel setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public List<String> getRegions() {
        return regions;
    }

    public Panel setRegions(List<String> regions) {
        this.regions = regions;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Panel setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
