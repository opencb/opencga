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
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DemoConfiguration {

    private Configuration configuration;
    private List<User> users;


    public static DemoConfiguration load(Path configurationPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        InputStream inputStream = FileUtils.newInputStream(configurationPath.resolve("main.yml"));
        DemoConfiguration demoConfiguration = objectMapper.readValue(inputStream, DemoConfiguration.class);
        Map<String, List<Study>> studies = new HashMap<>();
        for (User user : demoConfiguration.getUsers()) {
            for (Project project : user.getProjects()) {
                studies.put(project.getId(), new ArrayList<>());
                for (Study study : project.getStudies()) {
                    File file = configurationPath.resolve(study.getId() + ".yml").toFile();
                    studies.get(project.getId()).add(objectMapper.readValue(file, Study.class));
                }
            }
        }

        // Set studies
        for (User user : demoConfiguration.getUsers()) {
            for (Project project : user.getProjects()) {
                project.setStudies(studies.get(project.getId()));
            }
        }
        return demoConfiguration;
    }

    public void serialize(Path configurationPath) throws IOException {
        System.out.println("Not implemented yet");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DemoConfiguration{");
        sb.append("configuration=").append(configuration);
        sb.append(", users=").append(users);
        sb.append('}');
        return sb.toString();
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public DemoConfiguration setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public List<User> getUsers() {
        return users;
    }

    public DemoConfiguration setUsers(List<User> users) {
        this.users = users;
        return this;
    }
}
