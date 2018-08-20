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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class DiseasePanel extends PrivateStudyUid {

    private String id;
    private String name;
    private String uuid;

    private int release;
    private int version;

    private String author;
    private String creationDate;
    private String modificationDate;
    private Status status;
    private SourcePanel source;
    private String description;

    private List<OntologyTerm> phenotypes;

    private List<VariantPanel> variants;
    private List<GenePanel> genes;
    private List<RegionPanel> regions;

    private Map<String, Object> attributes;

    public DiseasePanel() {
    }

    public DiseasePanel(String id, String name, int version) {
        this.id = id;
        this.name = name;
        this.version = version;
    }

    public DiseasePanel(String id, String name, int release, int version, String author, SourcePanel source, String description,
                        List<OntologyTerm> phenotypes, List<VariantPanel> variants, List<GenePanel> genes, List<RegionPanel> regions,
                        Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.release = release;
        this.version = version;
        this.author = author;
        this.creationDate = TimeUtils.getTime();
        this.status = new Status();
        this.source = source;
        this.description = description;
        this.phenotypes = phenotypes;
        this.variants = variants;
        this.genes = genes;
        this.regions = regions;
        this.attributes = attributes;
    }

    // Json loader
    public static DiseasePanel load(InputStream diseasePanelInputStream) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(diseasePanelInputStream, DiseasePanel.class);
    }

    public static class SourcePanel {

        private String id;
        private String project;
        private String version;

        public SourcePanel() {
        }

        public SourcePanel(String id, String project, String version) {
            this.id = id;
            this.project = project;
            this.version = version;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SourcePanel{");
            sb.append("id='").append(id).append('\'');
            sb.append(", project='").append(project).append('\'');
            sb.append(", version='").append(version).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public SourcePanel setId(String id) {
            this.id = id;
            return this;
        }

        public String getProject() {
            return project;
        }

        public SourcePanel setProject(String project) {
            this.project = project;
            return this;
        }

        public String getVersion() {
            return version;
        }

        public SourcePanel setVersion(String version) {
            this.version = version;
            return this;
        }
    }

    public static class VariantPanel {

        private String id;
        private String phenotype;

        public VariantPanel() {
        }

        public VariantPanel(String id, String phenotype) {
            this.id = id;
            this.phenotype = phenotype;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("VariantPanel{");
            sb.append("id='").append(id).append('\'');
            sb.append(", phenotype='").append(phenotype).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public VariantPanel setId(String id) {
            this.id = id;
            return this;
        }

        public String getPhenotype() {
            return phenotype;
        }

        public VariantPanel setPhenotype(String phenotype) {
            this.phenotype = phenotype;
            return this;
        }
    }

    public static class GenePanel {

        /**
         * Ensembl ID is used as id
         */
        private String id;

        /**
         * HGNC Gene Symbol is used as name
         */
        private String name;
        private String confidence;

        public GenePanel() {
        }

        public GenePanel(String id, String name, String confidence) {
            this.id = id;
            this.name = name;
            this.confidence = confidence;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("GenePanel{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", confidence='").append(confidence).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public GenePanel setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public GenePanel setName(String name) {
            this.name = name;
            return this;
        }

        public String getConfidence() {
            return confidence;
        }

        public GenePanel setConfidence(String confidence) {
            this.confidence = confidence;
            return this;
        }
    }

    public static class RegionPanel {

        private String location;
        private float score;

        public RegionPanel() {
        }

        public RegionPanel(String location, float score) {
            this.location = location;
            this.score = score;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RegionPanel{");
            sb.append("location='").append(location).append('\'');
            sb.append(", score=").append(score);
            sb.append('}');
            return sb.toString();
        }

        public String getLocation() {
            return location;
        }

        public RegionPanel setLocation(String location) {
            this.location = location;
            return this;
        }

        public float getScore() {
            return score;
        }

        public RegionPanel setScore(float score) {
            this.score = score;
            return this;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DiseasePanel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", author='").append(author).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", source=").append(source);
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

    public DiseasePanel setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public DiseasePanel setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getName() {
        return name;
    }

    public DiseasePanel setName(String name) {
        this.name = name;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public DiseasePanel setVersion(int version) {
        this.version = version;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public DiseasePanel setRelease(int release) {
        this.release = release;
        return this;
    }

    public String getAuthor() {
        return author;
    }

    public DiseasePanel setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public DiseasePanel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public DiseasePanel setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public DiseasePanel setStatus(Status status) {
        this.status = status;
        return this;
    }

    public SourcePanel getSource() {
        return source;
    }

    public DiseasePanel setSource(SourcePanel source) {
        this.source = source;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DiseasePanel setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<OntologyTerm> getPhenotypes() {
        return phenotypes;
    }

    public DiseasePanel setPhenotypes(List<OntologyTerm> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<VariantPanel> getVariants() {
        return variants;
    }

    public DiseasePanel setVariants(List<VariantPanel> variants) {
        this.variants = variants;
        return this;
    }

    public List<GenePanel> getGenes() {
        return genes;
    }

    public DiseasePanel setGenes(List<GenePanel> genes) {
        this.genes = genes;
        return this;
    }

    public List<RegionPanel> getRegions() {
        return regions;
    }

    public DiseasePanel setRegions(List<RegionPanel> regions) {
        this.regions = regions;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public DiseasePanel setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
