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

package com.zettagenomics.opencga.enterprise.cvdb.parsers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.project.Project;

import java.util.HashMap;
import java.util.Map;

public class CollectionPrefixUtils {

    public static final String OPENCGA_CVDB_DBPREFIX_KEY = "OPENCGA_CVDB_DBPREFIX";
    public static final String CVDB_SEP = "_";

    private static CollectionPrefixUtils instance;
    private Map<String, String> collectionPrefixMap;
    private CatalogManager catalogManager;

    private CollectionPrefixUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        this.collectionPrefixMap = new HashMap<>();
    }

    public static synchronized CollectionPrefixUtils getInstance(CatalogManager catalogManager) {
        if (instance == null) {
            instance = new CollectionPrefixUtils(catalogManager);
        }
        return instance;
    }

    public String getCollectionPrefix(String organizationId, String projectId, String token) throws CatalogException {
        String key = organizationId + "===" + projectId;
        if (!collectionPrefixMap.containsKey(key)) {
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.INTERNAL_DATASTORES_CVDB.key());
            Project project = catalogManager.getProjectManager().get(projectId, queryOptions, token).first();
            String collectionPrefix = null;
            if (project.getInternal() != null && project.getInternal().getDatastores() != null
                    && project.getInternal().getDatastores().getCvdb() != null) {
                collectionPrefix = project.getInternal().getDatastores().getCvdb().getDbName();
            }
            if (StringUtils.isEmpty(collectionPrefix)) {
                collectionPrefix = VariantStorageManager.buildDatabaseName(catalogManager.getConfiguration().getDatabasePrefix(), "cvdb",
                        organizationId, projectId);
            }

            collectionPrefixMap.put(key, collectionPrefix);
        }

        return collectionPrefixMap.get(key);
    }

    public static String getCollectionName(String prefix, String suffix) {
        return prefix + CVDB_SEP + suffix;
    }
}
