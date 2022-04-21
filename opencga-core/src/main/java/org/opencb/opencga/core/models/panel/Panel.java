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

package org.opencb.opencga.core.models.panel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.core.OntologyTerm;
import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.common.InternalStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

@DataClass(id = "Panel", since = "1.0",
        description = "Panel data model hosts information about any panel.")
public class Panel extends DiseasePanel implements IPrivateStudyUid {

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;
    /**
     * OpenCGA version of this panel, this is incremented when the panel is updated.
     */
    @DataField(id = "version", indexed = true,
            description = FieldConstants.PANEL_VERSION_DESCRIPTION)
    private int version;

    @DataField(id = "author", indexed = true, deprecated = true,
            description = FieldConstants.PANEL_AUTHOR_DESCRIPTION)
    @Deprecated
    private String author;


    @DataField(id = "status", indexed = true,
            description = FieldConstants.PANEL_STATUS_DESCRIPTION)
    private Status status;

    // Private fields
    @DataField(id = "studyUid", indexed = true,
            description = FieldConstants.PANEL_STUDY_UID_DESCRIPTION)
    private long studyUid;

    @DataField(id = "uid", indexed = true,
            description = FieldConstants.PANEL_STUDY_UID_DESCRIPTION)
    private long uid;

    public Panel() {
    }

    public Panel(String id, String name, int version) {
        super(id, name);
        this.version = version;
    }

    public Panel(String id, String name, List<PanelCategory> categories, List<OntologyTerm> disorders, List<String> tags,
                 List<VariantPanel> variants, List<GenePanel> genes, List<RegionPanel> regions,
                 List<STR> strs, Map<String, Integer> stats, int release, int version, String author, SourcePanel source, InternalStatus status,
                 String description, Map<String, Object> attributes) {
        super(id, name, categories, disorders, tags, variants, genes, strs, regions, stats, source, TimeUtils.getTime(),
                TimeUtils.getTime(), description, attributes);
        this.release = release;
        this.version = version;
        this.author = author;
        this.status = status;

        if (StringUtils.isNotEmpty(author) && source != null && StringUtils.isEmpty(source.getAuthor())) {
            this.getSource().setAuthor(author);
        }
    }

    /**
     * Static method to load and parse a JSON string from an InputStream.
     *
     * @param diseasePanelInputStream InputStream with the JSON string representing this panel.
     * @return A DiseasePanel object.
     * @throws IOException Propagate Jackson IOException.
     */
    public static Panel load(InputStream diseasePanelInputStream) throws IOException {
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        Panel panel = objectMapper.readValue(diseasePanelInputStream, Panel.class);
//        // By default, the id will be loaded within the Panel and not the DiseasePanel class
//        panel.getDiseasePanel().setId(panel.getId());
        return panel;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DiseasePanel{");
        sb.append("id='").append(getId()).append('\'');
        sb.append(", name='").append(getName()).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", categories=").append(getCategories());
        sb.append(", disorders=").append(getDisorders());
        sb.append(", tags=").append(getTags());
        sb.append(", variants=").append(getVariants());
        sb.append(", genes=").append(getGenes());
        sb.append(", regions=").append(getRegions());
        sb.append(", stats=").append(getStats());
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", author='").append(author).append('\'');
        sb.append(", source=").append(getSource());
        sb.append(", status=").append(status);
        sb.append(", creationDate='").append(getCreationDate()).append('\'');
        sb.append(", modificationDate='").append(getModificationDate()).append('\'');
        sb.append(", description='").append(getDescription()).append('\'');
        sb.append(", attributes=").append(getAttributes());
        sb.append('}');
        return sb.toString();
    }

    public Panel setId(String id) {
        super.setId(id);
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

    @Override
    public long getStudyUid() {
        return studyUid;
    }

    @Override
    public Panel setStudyUid(long studyUid) {
        this.studyUid = studyUid;
        return this;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public Panel setUid(long uid) {
        this.uid = uid;
        return this;
    }

    @Override
    public Panel setGenes(List<GenePanel> genes) {
        super.setGenes(genes);
        return this;
    }
}
