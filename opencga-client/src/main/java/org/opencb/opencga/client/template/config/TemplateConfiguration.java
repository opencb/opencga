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

package org.opencb.opencga.client.template.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TemplateConfiguration {

    private String version;
    private String baseUrl;
    private boolean index;
    private List<StudyAclEntry> acl;
    private List<Project> projects;


    public static TemplateConfiguration load(Path mainConfigurationPath) throws IOException {
        mainConfigurationPath = mainConfigurationPath.toAbsolutePath();
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        TemplateConfiguration templateConfiguration;
        try {
            templateConfiguration = objectMapper.readValue(mainConfigurationPath.toFile(), TemplateConfiguration.class);
        } catch (IOException e) {
            // Enrich IOException with fileName
            throw new IOException("Error parsing main template configuration file '" + mainConfigurationPath + "'", e);
        }
        Map<String, List<Study>> studies = new HashMap<>();
        for (Project project : templateConfiguration.getProjects()) {
            studies.put(project.getId(), new ArrayList<>());
            if (project.getStudies() != null) {
                for (Study study : project.getStudies()) {
                    File file = mainConfigurationPath.getParent().resolve(study.getId() + ".yml").toFile();
                    // If file exists we load it and overwrite Study object.
                    // If a file with the study does not exist then Study must be defined in the main.yml file.
                    if (file.exists()) {
                        try {
                            studies.get(project.getId()).add(objectMapper.readValue(file, Study.class));
                        } catch (IOException e) {
                            // Enrich IOException with fileName
                            throw new IOException("Error parsing study '" + study.getId() + "' template configuration file "
                                    + "'" + mainConfigurationPath + "'", e);
                        }
                    }
                }
            }
        }

        // Set studies read from files
        for (Project project : templateConfiguration.getProjects()) {
            project.setStudies(studies.get(project.getId()));
        }
        return templateConfiguration;
    }

    public void serialize(Path configurationPath) throws IOException {
        System.out.println("Not implemented yet");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TemplateConfiguration{");
        sb.append("version='").append(version).append('\'');
        sb.append(", baseUrl='").append(baseUrl).append('\'');
        sb.append(", index=").append(index);
        sb.append(", acl=").append(acl);
        sb.append(", projects=").append(projects);
        sb.append('}');
        return sb.toString();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public boolean isIndex() {
        return index;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public List<StudyAclEntry> getAcl() {
        return acl;
    }

    public void setAcl(List<StudyAclEntry> acl) {
        this.acl = acl;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }
}
