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

package org.opencb.opencga.core.models.project;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.PrivateFields;
import org.opencb.opencga.core.models.study.Study;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
@DataClass(id = "Project", since = "1.0",
        description = "Project data model hosts information about any project.")
public class Project extends PrivateFields {

    /**
     * Project ID is a mandatory parameter when creating a new sample, this ID cannot be changed at the moment.
     *
     * @apiNote Required, Immutable, Unique
     */
    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    /**
     * Global unique ID at the whole OpenCGA installation. This is automatically created during the Project creation and cannot be changed.
     *
     * @apiNote Internal, Unique, Immutable
     */
    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;


    @DataField(id = "name", indexed = true,
            description = FieldConstants.GENERIC_NAME)
    private String name;

    /**
     * Full Qualified Name (user@projectId).
     */

    @DataField(id = "fqn", indexed = true,
            description = FieldConstants.PROJECT_FQN)
    private String fqn;

    /**
     * String representing when the sample was created, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "creationDate", indexed = true,
            description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    /**
     * String representing when was the last time the sample was modified, this is automatically set by OpenCGA.
     *
     * @apiNote Internal
     */
    @DataField(id = "modificationDate", indexed = true, since = "1.0",
            description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    /**
     * An string to describe the properties of the sample.
     *
     * @apiNote
     */
    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "organism",
            description = FieldConstants.PROJECT_ORGANISM)
    private ProjectOrganism organism;


    @DataField(id = "currentRelease", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int currentRelease;


    @DataField(id = "studies",
            description = FieldConstants.PROJECT_STUDIES)
    private List<Study> studies;

    /**
     * An object describing the internal information of the Project. This is managed by OpenCGA.
     *
     * @apiNote Internal
     */

    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private ProjectInternal internal;

    /**
     * You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.
     *
     * @apiNote
     */
    @DataField(id = "attributes", indexed = true,
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public Project() {
    }

    public Project(String id, String name, String description, ProjectOrganism organism, int currentRelease, ProjectInternal internal) {
        this(id, name, TimeUtils.getTime(), TimeUtils.getTime(), description, organism, new LinkedList<>(), currentRelease, internal,
                new HashMap<>());
    }

    public Project(String id, String name, String creationDate, String modificationDate, String description, ProjectOrganism organism,
                   int currentRelease, ProjectInternal internal) {
        this(id, name, creationDate, modificationDate, description, organism, new LinkedList<>(), currentRelease, internal,
                new HashMap<>());
    }

    public Project(String id, String name, String creationDate, String modificationDate, String description, ProjectOrganism organism,
                   List<Study> studies, int currentRelease, ProjectInternal internal, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.organism = organism;
        this.studies = studies;
        this.currentRelease = currentRelease;
        this.internal = internal;
        this.attributes = attributes;
    }

    // Clone a project
    public Project(Project project) {
        this(project.getUid(), project.getId(), project.getName(), project.getUuid(), project.getFqn(), project.getCreationDate(),
                project.getModificationDate(), project.getDescription(), project.getOrganism(), project.getCurrentRelease(),
                project.getStudies(), project.getInternal(), project.getAttributes());
    }

    public Project(long uid, String id, String name, String uuid, String fqn, String creationDate, String modificationDate,
                   String description, ProjectOrganism organism, int currentRelease, List<Study> studies, ProjectInternal internal,
                   Map<String, Object> attributes) {
        super(uid);
        this.id = id;
        this.name = name;
        this.uuid = uuid;
        this.fqn = fqn;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.description = description;
        this.organism = organism;
        this.currentRelease = currentRelease;
        this.studies = studies;
        this.internal = internal;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Project{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", fqn='").append(fqn).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", organism=").append(organism);
        sb.append(", currentRelease=").append(currentRelease);
        sb.append(", internal=").append(internal);
        sb.append(", studies=").append(studies);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public Project setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Project setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public Project setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    public String getName() {
        return name;
    }

    public Project setName(String name) {
        this.name = name;
        return this;
    }

    public String getFqn() {
        return fqn;
    }

    public Project setFqn(String fqn) {
        this.fqn = fqn;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Project setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Project setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public int getCurrentRelease() {
        return currentRelease;
    }

    public Project setCurrentRelease(int currentRelease) {
        this.currentRelease = currentRelease;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Project setDescription(String description) {
        this.description = description;
        return this;
    }

    public ProjectOrganism getOrganism() {
        return organism;
    }

    public Project setOrganism(ProjectOrganism organism) {
        this.organism = organism;
        return this;
    }

    public ProjectInternal getInternal() {
        return internal;
    }

    public Project setInternal(ProjectInternal internal) {
        this.internal = internal;
        return this;
    }

    public List<Study> getStudies() {
        return studies;
    }

    public Project setStudies(List<Study> studies) {
        this.studies = studies;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Project setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
