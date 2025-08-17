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

package org.opencb.opencga.analysis.wrappers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WrapperUtils {

    public static final String INPUT_FILE_PREFIX = "input://";
    public static final String OUTPUT_FILE_PREFIX = "output://";

    private static Logger logger = LoggerFactory.getLogger(WrapperUtils.class);

    private WrapperUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static String checkPath(String fileId, String study, CatalogManager catalogManager, String token) throws ToolException {
        try {
            File file = catalogManager.getFileManager().get(study, fileId, QueryOptions.empty(), token)
                    .first();
            Path path = Paths.get(file.getUri().getPath());
            if (Files.exists(path)) {
                return INPUT_FILE_PREFIX + path.toAbsolutePath();
            } else {
                throw new ToolException("File '" + fileId + "' does not exist in the local filesystem: " + path.toAbsolutePath());
            }
        } catch (CatalogException e) {
            throw new ToolException("Error checking file '" + fileId + "'", e);
        }
    }

    public static List<String> checkPaths(List<String> paths, String study, CatalogManager catalogManager, String token)
            throws ToolException {
        List<String> updatedPaths = new ArrayList<>(paths.size());
        for (String path : paths) {
            updatedPaths.add(WrapperUtils.checkPath(path, study, catalogManager, token));
        }
        return updatedPaths;
    }

    @Deprecated
    public static ObjectMap checkParams(ObjectMap params, String study, CatalogManager catalogManager, String token)
            throws ToolException {
        return checkParams(params, Collections.emptyList(), Collections.emptyList(), study, catalogManager, token);
    }

    public static ObjectMap checkParams(ObjectMap params, List<String> fileParams, List<String> skipParams, String study,
                                        CatalogManager catalogManager, String token) throws ToolException {
        ObjectMap updatedParams = new ObjectMap();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (skipParams.contains(entry.getKey())) {
                // Skip this parameter
                logger.info("Skipping parameter '{}' since it will be set later or ignored", entry.getKey());
                continue;
            }
            if (fileParams.contains(entry.getKey())) {
                if (entry.getValue() instanceof String) {
                    updatedParams.put(entry.getKey(), WrapperUtils.checkPath((String) entry.getValue(), study, catalogManager, token));
                } else {
                    throw new ToolException("Parameter '" + entry.getKey() + "' should be a String representing a file path, but found: "
                            + entry.getValue().getClass().getSimpleName());
                }
            } else {
                // Otherwise, we assume it's a regular parameter
                updatedParams.put(entry.getKey(), entry.getValue());
            }
        }
        return updatedParams;
    }
}
