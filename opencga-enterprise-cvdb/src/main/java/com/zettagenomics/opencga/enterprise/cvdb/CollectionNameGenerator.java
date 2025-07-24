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

package com.zettagenomics.opencga.enterprise.cvdb;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.project.Project;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.PROJECT_PARAM_NAME;

public class CollectionNameGenerator {

    public static final String CLINICAL_ANALYSES_COLLECTION_SUFFIX = "analyses";
    public static final String INTERPRETATIONS_COLLECTION_SUFFIX = "interpretations";
    public static final String CLINICAL_VARIANTS_COLLECTION_SUFFIX = "variants";
    public static final String CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX = "evidences";
    public static final String CLINICAL_VIEWERS_COLLECTION_SUFFIX = "viewers";

    protected static final List<String> COLLECTION_SUFFIXES = Arrays.asList(CLINICAL_ANALYSES_COLLECTION_SUFFIX,
            INTERPRETATIONS_COLLECTION_SUFFIX,
            CLINICAL_VARIANTS_COLLECTION_SUFFIX,
            CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX,
            CLINICAL_VIEWERS_COLLECTION_SUFFIX);

    public static final String OPENCGA_CVDB_DBPREFIX_KEY = "OPENCGA_CVDB_DBPREFIX";
    public static final String CVDB_SEP = "_";

    private CatalogManager catalogManager;
    private Map<String, String> collectionPrefixMap;

    public CollectionNameGenerator(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        this.collectionPrefixMap = new ConcurrentHashMap<>();
    }

    public String getCollectionPrefix(Query query, String token) throws CatalogException {
        String projectId = query.getString(PROJECT_PARAM_NAME);
        if (StringUtils.isEmpty(projectId)) {
            throw new CatalogException("Missing project ID in query: " + query);
        }

        // Get organization
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = jwtPayload.getOrganization();

        return getCollectionPrefix(organizationId, projectId, token);
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

    public List<String> getCollectionSuffixes() {
        return COLLECTION_SUFFIXES;
    }

    public List<String> getCollectionNames(String collectionPrefix) {
        return COLLECTION_SUFFIXES.stream()
                .map(suffix -> getCollectionName(collectionPrefix, suffix)).collect(Collectors.toList());
    }

    public String getClinicalAnalysisCollectionName(Query query, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(query, token), CLINICAL_ANALYSES_COLLECTION_SUFFIX);
    }

    public String getClinicalAnalysisCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_ANALYSES_COLLECTION_SUFFIX);
    }

    public String getClinicalInterpretationCollectionName(Query query, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(query, token), INTERPRETATIONS_COLLECTION_SUFFIX);
    }

    public String getClinicalInterpretationCollectionName(String prefix) {
        return getCollectionName(prefix, INTERPRETATIONS_COLLECTION_SUFFIX);
    }

    public String getClinicalVariantCollectionName(Query query, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(query, token), CLINICAL_VARIANTS_COLLECTION_SUFFIX);
    }

    public String getClinicalVariantCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_VARIANTS_COLLECTION_SUFFIX);
    }

    public String getClinicalVariantEvidenceCollectionName(Query query, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(query, token), CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
    }

    public String getClinicalVariantEvidenceCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
    }

    public String getClinicalViewerCollectionName(Query query, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(query, token), CLINICAL_VIEWERS_COLLECTION_SUFFIX);
    }

    public String getClinicalViewerCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_VIEWERS_COLLECTION_SUFFIX);
    }

    public String getCollectionName(String prefix, String suffix) {
        return prefix + CVDB_SEP + suffix;
    }
}
