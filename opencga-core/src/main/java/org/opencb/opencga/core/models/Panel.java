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
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.*;

public class Panel extends PrivateStudyUid {

    private DiseasePanel diseasePanel;

    private String uuid;
    private int release;
    /**
     * OpenCGA version of this panel, this is incremented when the panel is updated.
     */
    private int version;

    @Deprecated
    private String author;
    private Status status;

    public Panel() {
        this.diseasePanel = new DiseasePanel();
    }

    public Panel(String id, String name, int version) {
        this.diseasePanel = new DiseasePanel(id, name);
        this.version = version;
    }

    public Panel(String id, String name, List<PanelCategory> categories, List<Phenotype> phenotypes, List<String> tags,
                 List<VariantPanel> variants, List<GenePanel> genes, List<RegionPanel> regions,
                 Map<String, Integer> stats, int release, int version, String author, SourcePanel source, Status status,
                 String description, Map<String, Object> attributes) {
        this.diseasePanel = new DiseasePanel(id, name, categories, phenotypes, tags,
                variants, genes, regions, stats, source, TimeUtils.getTime(), TimeUtils.getTime(), description, attributes);
        this.release = release;
        this.version = version;
        this.author = author;
        this.status = status;

        if (StringUtils.isNotEmpty(author) && source != null && StringUtils.isEmpty(source.getAuthor())) {
            diseasePanel.getSource().setAuthor(author);
        }
    }

    /**
     * Static method to load and parse a JSON string from an InputStream.
     * @param diseasePanelInputStream InputStream with the JSON string representing this panel.
     * @return A DiseasePanel object.
     * @throws IOException Propagate Jackson IOException.
     */
    public static Panel load(InputStream diseasePanelInputStream) throws IOException {
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        return objectMapper.readValue(diseasePanelInputStream, Panel.class);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DiseasePanel{");
        sb.append("id='").append(diseasePanel.getId()).append('\'');
        sb.append(", name='").append(diseasePanel.getName()).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", categories=").append(diseasePanel.getCategories());
        sb.append(", phenotypes=").append(diseasePanel.getPhenotypes());
        sb.append(", tags=").append(diseasePanel.getTags());
        sb.append(", variants=").append(diseasePanel.getVariants());
        sb.append(", genes=").append(diseasePanel.getGenes());
        sb.append(", regions=").append(diseasePanel.getRegions());
        sb.append(", stats=").append(diseasePanel.getStats());
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", author='").append(author).append('\'');
        sb.append(", source=").append(diseasePanel.getSource());
        sb.append(", status=").append(status);
        sb.append(", creationDate='").append(diseasePanel.getCreationDate()).append('\'');
        sb.append(", modificationDate='").append(diseasePanel.getModificationDate()).append('\'');
        sb.append(", description='").append(diseasePanel.getDescription()).append('\'');
        sb.append(", attributes=").append(diseasePanel.getAttributes());
        sb.append('}');
        return sb.toString();
    }

    public DiseasePanel getDiseasePanel() {
        return diseasePanel;
    }

    public Panel setDiseasePanel(DiseasePanel diseasePanel) {
        this.diseasePanel = diseasePanel;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Panel setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Panel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Panel setVersion(int version) {
        this.version = version;
        return this;
    }

    @Deprecated
    public String getAuthor() {
        return author;
    }

    @Deprecated
    public Panel setAuthor(String author) {
        this.author = author;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Panel setStatus(Status status) {
        this.status = status;
        return this;
    }

}
