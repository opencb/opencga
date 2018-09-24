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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.Xref;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class DiseasePanel extends PrivateStudyUid {

    private String id;
    private String name;
    private String uuid;

    private List<PanelCategory> categories;
    private List<Phenotype> phenotypes;
    private List<String> tags;

    private List<VariantPanel> variants;
    private List<GenePanel> genes;
    private List<RegionPanel> regions;
    private Map<String, Integer> stats;

    private int release;
    /**
     * OpenCGA version of this panel, this is incremented when the panel is updated.
     */
    private int version;

    @Deprecated
    private String author;

    /**
     * Information taken from the source of this panel.
     * For instance if the panel is taken from PanelApp this will contain the id, name and version in PanelApp.
     */
    private SourcePanel source;
    private Status status;
    private String creationDate;
    private String modificationDate;
    private String description;

    private Map<String, Object> attributes;


    public DiseasePanel() {
    }

    public DiseasePanel(String id, String name, int version) {
        this.id = id;
        this.name = name;
        this.version = version;
    }

    @Deprecated
    public DiseasePanel(String id, String name, int release, int version, String author, SourcePanel source, String description,
                        List<Phenotype> phenotypes, List<VariantPanel> variants, List<GenePanel> genes, List<RegionPanel> regions,
                        List<String> tags, Map<String, Object> attributes) {
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
        this.tags = tags;
        this.attributes = attributes;
    }

    public DiseasePanel(String id, String name, List<PanelCategory> categories, List<Phenotype> phenotypes, List<String> tags,
                        List<VariantPanel> variants, List<GenePanel> genes, List<RegionPanel> regions, Map<String, Integer> stats,
                        int release, int version, String author, SourcePanel source, Status status, String description,
                        Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.categories = categories;
        this.phenotypes = phenotypes;
        this.tags = tags;
        this.variants = variants;
        this.genes = genes;
        this.regions = regions;
        this.stats = stats;
        this.release = release;
        this.version = version;
        this.author = author;
        this.source = source;
        this.status = status;
        this.creationDate = TimeUtils.getTime();
        this.modificationDate = TimeUtils.getTime();
        this.description = description;
        this.attributes = attributes;

        if (StringUtils.isNotEmpty(author) && source != null && StringUtils.isEmpty(source.getAuthor())) {
            this.source.setAuthor(author);
        }
    }

    /**
     * Static method to load and parse a JSON string from an InputStream.
     * @param diseasePanelInputStream InputStream with the JSON string representing this panel.
     * @return A DiseasePanel object.
     * @throws IOException Propagate Jackson IOException.
     */
    public static DiseasePanel load(InputStream diseasePanelInputStream) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(diseasePanelInputStream, DiseasePanel.class);
    }


    public static class PanelCategory {

        private String name;
        private int level;

        public PanelCategory() {
        }

        public PanelCategory(String name, int level) {
            this.name = name;
            this.level = level;
        }

        public String getName() {
            return name;
        }

        public PanelCategory setName(String name) {
            this.name = name;
            return this;
        }

        public int getLevel() {
            return level;
        }

        public PanelCategory setLevel(int level) {
            this.level = level;
            return this;
        }
    }

    public static class SourcePanel {

        private String id;
        private String name;
        private String version;
        private String author;
        private String project;

        public SourcePanel() {
        }

        @Deprecated
        public SourcePanel(String id, String name, String version, String project) {
            this.id = id;
            this.name = name;
            this.version = version;
            this.project = project;
        }

        public SourcePanel(String id, String name, String version, String author, String project) {
            this.id = id;
            this.name = name;
            this.version = version;
            this.author = author;
            this.project = project;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SourcePanel{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", version='").append(version).append('\'');
            sb.append(", author='").append(author).append('\'');
            sb.append(", project='").append(project).append('\'');
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

        public String getName() {
            return name;
        }

        public SourcePanel setName(String name) {
            this.name = name;
            return this;
        }

        public String getVersion() {
            return version;
        }

        public SourcePanel setVersion(String version) {
            this.version = version;
            return this;
        }

        public String getAuthor() {
            return author;
        }

        public SourcePanel setAuthor(String author) {
            this.author = author;
            return this;
        }

        public String getProject() {
            return project;
        }

        public SourcePanel setProject(String project) {
            this.project = project;
            return this;
        }
    }

    public static class VariantPanel {

        private String id;
        private List<String> evidences;
        private List<String> publications;

        public VariantPanel() {
        }

        public VariantPanel(String id, String phenotype, List<String> evidences, List<String> publications) {
            this.id = id;
            this.evidences = evidences;
            this.publications = publications;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("VariantPanel{");
            sb.append("id='").append(id).append('\'');
            sb.append(", evidences=").append(evidences);
            sb.append(", publications=").append(publications);
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

        public List<String> getEvidences() {
            return evidences;
        }

        public VariantPanel setEvidences(List<String> evidences) {
            this.evidences = evidences;
            return this;
        }

        public List<String> getPublications() {
            return publications;
        }

        public VariantPanel setPublications(List<String> publications) {
            this.publications = publications;
            return this;
        }
    }


    public static class GenePanel {

        /**
         * Ensembl ID is used as id.
         */
        private String id;

        /**
         * HGNC Gene Symbol is used as name.
         */
        private String name;
        private List<Xref> xrefs;
        private String modeOfInheritance;
        private String confidence;
        private List<String> evidences;
        private List<String> publications;

        public GenePanel() {
        }

        public GenePanel(String id, String name, List<Xref> xrefs, String modeOfInheritance, String confidence, List<String> evidences,
                         List<String> publications) {
            this.id = id;
            this.name = name;
            this.xrefs = xrefs;
            this.modeOfInheritance = modeOfInheritance;
            this.confidence = confidence;
            this.evidences = evidences;
            this.publications = publications;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("GenePanel{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", xrefs=").append(xrefs);
            sb.append(", modeOfInheritance='").append(modeOfInheritance).append('\'');
            sb.append(", confidence='").append(confidence).append('\'');
            sb.append(", evidences=").append(evidences);
            sb.append(", publications=").append(publications);
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

        public List<Xref> getXrefs() {
            return xrefs;
        }

        public GenePanel setXrefs(List<Xref> xrefs) {
            this.xrefs = xrefs;
            return this;
        }

        public String getModeOfInheritance() {
            return modeOfInheritance;
        }

        public GenePanel setModeOfInheritance(String modeOfInheritance) {
            this.modeOfInheritance = modeOfInheritance;
            return this;
        }

        public String getConfidence() {
            return confidence;
        }

        public GenePanel setConfidence(String confidence) {
            this.confidence = confidence;
            return this;
        }

        public List<String> getEvidences() {
            return evidences;
        }

        public GenePanel setEvidences(List<String> evidences) {
            this.evidences = evidences;
            return this;
        }

        public List<String> getPublications() {
            return publications;
        }

        public GenePanel setPublications(List<String> publications) {
            this.publications = publications;
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
        sb.append(", categories=").append(categories);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", tags=").append(tags);
        sb.append(", variants=").append(variants);
        sb.append(", genes=").append(genes);
        sb.append(", regions=").append(regions);
        sb.append(", stats=").append(stats);
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", author='").append(author).append('\'');
        sb.append(", source=").append(source);
        sb.append(", status=").append(status);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
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

    public String getName() {
        return name;
    }

    public DiseasePanel setName(String name) {
        this.name = name;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public DiseasePanel setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public List<PanelCategory> getCategories() {
        return categories;
    }

    public DiseasePanel setCategories(List<PanelCategory> categories) {
        this.categories = categories;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public DiseasePanel setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public DiseasePanel setTags(List<String> tags) {
        this.tags = tags;
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

    public Map<String, Integer> getStats() {
        return stats;
    }

    public DiseasePanel setStats(Map<String, Integer> stats) {
        this.stats = stats;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public DiseasePanel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public DiseasePanel setVersion(int version) {
        this.version = version;
        return this;
    }

    @Deprecated
    public String getAuthor() {
        return author;
    }

    @Deprecated
    public DiseasePanel setAuthor(String author) {
        this.author = author;
        return this;
    }

    public SourcePanel getSource() {
        return source;
    }

    public DiseasePanel setSource(SourcePanel source) {
        this.source = source;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public DiseasePanel setStatus(Status status) {
        this.status = status;
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

    public String getDescription() {
        return description;
    }

    public DiseasePanel setDescription(String description) {
        this.description = description;
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
