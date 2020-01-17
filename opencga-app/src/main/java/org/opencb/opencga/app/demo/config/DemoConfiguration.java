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

package org.opencb.opencga.app.demo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DemoConfiguration {

    private String url;

    private List<User> users;

    private static final String DEFAULT_CONFIGURATION_FORMAT = "yml";

    public static DemoConfiguration load(Path configurationPath) throws IOException {
        InputStream inputStream = FileUtils.newInputStream(configurationPath);
        return load(inputStream, DEFAULT_CONFIGURATION_FORMAT);
    }

    public static DemoConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_CONFIGURATION_FORMAT);
    }

    public static DemoConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        DemoConfiguration demoConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                demoConfiguration = objectMapper.readValue(configurationInputStream, DemoConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                demoConfiguration = objectMapper.readValue(configurationInputStream, DemoConfiguration.class);
                Map<String, List<Study>> studies = new HashMap<>();
                for (User user : demoConfiguration.getUsers()) {
                    for (Project project : user.getProjects()) {
                        studies.put(project.getId(), new ArrayList<>());
                        for (Study study : project.getStudies()) {
                            studies.get(project.getId()).add(objectMapper.readValue(new File(study.getId() + ".yml"), Study.class));
                        }
                    }
                }

                // Set sutdies
                for (User user : demoConfiguration.getUsers()) {
                    for (Project project : user.getProjects()) {
                        project.setStudies(studies.get(project.getId()));
                    }
                }

                break;
        }
        return demoConfiguration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    @Override
    public String toString() {
        return "DemoConfiguration{" +
                "url='" + url + '\'' +
                '}';
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        DemoConfiguration.users = users;
    }
}
