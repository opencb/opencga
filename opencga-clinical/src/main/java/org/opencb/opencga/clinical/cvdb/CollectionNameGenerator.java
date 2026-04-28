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

package org.opencb.opencga.clinical.cvdb;

import org.apache.commons.lang3.StringUtils;
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

    public static final String CVDB_SEP = "_";

    private CatalogManager catalogManager;
    private Map<String, String> collectionPrefixMap;

    public CollectionNameGenerator(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        this.collectionPrefixMap = new ConcurrentHashMap<>();
    }

    public String getCollectionPrefix(String projectId, String token) throws CatalogException {
        if (StringUtils.isEmpty(projectId)) {
            throw new CatalogException("Could not get CVDB collection prefix since project ID is empty");
        }

        // Get organization
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = jwtPayload.getOrganization();

        return getCollectionPrefix(organizationId, projectId, token);
    }

    public String getCollectionPrefix(String organizationId, String projectId, String token) throws CatalogException {
        String key = organizationId + ":" + projectId;
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

    public static List<String> getCollectionSuffixes() {
        return COLLECTION_SUFFIXES;
    }

    public List<String> getCollectionNames(String collectionPrefix) {
        return COLLECTION_SUFFIXES.stream()
                .map(suffix -> getCollectionName(collectionPrefix, suffix)).collect(Collectors.toList());
    }

    public String getClinicalAnalysisCollectionName(String projectId, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(projectId, token), CLINICAL_ANALYSES_COLLECTION_SUFFIX);
    }

    public static String getClinicalAnalysisCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_ANALYSES_COLLECTION_SUFFIX);
    }

    public String getClinicalInterpretationCollectionName(String projectId, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(projectId, token), INTERPRETATIONS_COLLECTION_SUFFIX);
    }

    public static String getClinicalInterpretationCollectionName(String prefix) {
        return getCollectionName(prefix, INTERPRETATIONS_COLLECTION_SUFFIX);
    }

    public String getClinicalVariantCollectionName(String projectId, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(projectId, token), CLINICAL_VARIANTS_COLLECTION_SUFFIX);
    }

    public static String getClinicalVariantCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_VARIANTS_COLLECTION_SUFFIX);
    }

    public String getClinicalVariantEvidenceCollectionName(String projectId, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(projectId, token), CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
    }

    public static String getClinicalVariantEvidenceCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX);
    }

    public String getClinicalViewerCollectionName(String projectId, String token) throws CatalogException {
        return getCollectionName(getCollectionPrefix(projectId, token), CLINICAL_VIEWERS_COLLECTION_SUFFIX);
    }

    public static String getClinicalViewerCollectionName(String prefix) {
        return getCollectionName(prefix, CLINICAL_VIEWERS_COLLECTION_SUFFIX);
    }

    public static String getCollectionName(String prefix, String suffix) {
        return prefix + CVDB_SEP + suffix;
    }
}
