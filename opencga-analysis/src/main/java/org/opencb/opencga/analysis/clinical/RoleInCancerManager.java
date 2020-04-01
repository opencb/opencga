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

package org.opencb.opencga.analysis.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.commons.utils.FileUtils.newBufferedReader;

public class RoleInCancerManager {
    private Path path;
    private static Map<String, ClinicalProperty.RoleInCancer> roleInCancer = null;

    public RoleInCancerManager(Path path) {
        this.path = path;
    }

    public Map<String, ClinicalProperty.RoleInCancer> getRoleInCancer() throws IOException {
        // Lazy loading
        if (roleInCancer == null) {
            roleInCancer = loadRoleInCancer();
        }
        return roleInCancer;
    }

    private Map<String, ClinicalProperty.RoleInCancer> loadRoleInCancer() throws IOException {
        try {
            FileUtils.checkFile(path);
        } catch (Exception e) {
            return null;
        }

        Map<String, ClinicalProperty.RoleInCancer> roleInCancer = new HashMap<>();

        BufferedReader bufferedReader = newBufferedReader(path);
        List<String> lines = bufferedReader.lines().collect(Collectors.toList());
        Set<ClinicalProperty.RoleInCancer> set = new HashSet<>();
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }

            set.clear();
            String[] split = line.split("\t");
            // Sanity check
            if (split.length > 1) {
                String[] roles = split[1].replace("\"", "").split(",");
                for (String role : roles) {
                    switch (role.trim().toLowerCase()) {
                        case "oncogene":
                            set.add(ClinicalProperty.RoleInCancer.ONCOGENE);
                            break;
                        case "tsg":
                            set.add(ClinicalProperty.RoleInCancer.TUMOR_SUPPRESSOR_GENE);
                            break;
                        default:
                            break;
                    }
                }
            }

            // Update set
            if (set.size() > 0) {
                if (set.size() == 2) {
                    roleInCancer.put(split[0], ClinicalProperty.RoleInCancer.BOTH);
                } else {
                    roleInCancer.put(split[0], set.iterator().next());
                }
            }
        }

        return roleInCancer;
    }
}
