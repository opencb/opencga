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

package org.opencb.opencga.analysis.family;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = FamilyIndexTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION, description = "Index Family entries in Solr.")
public class FamilyIndexTask extends OpenCgaTool {

    public final static String ID = "family-secondary-index";

    private CatalogSolrManager catalogSolrManager;

    @Override
    protected void check() throws Exception {
        catalogSolrManager = new CatalogSolrManager(this.catalogManager);
    }

    @Override
    protected void run() throws Exception {
//        // Get all the studies
//        Query query = new Query();
//        QueryOptions options = new QueryOptions()
//                .append(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
//                        StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.FQN.key(),
//                        StudyDBAdaptor.QueryParams.VARIABLE_SET.key()))
//                .append(DBAdaptor.INCLUDE_ACLS, true);
//        OpenCGAResult<Study> studyDataResult = catalogManager.getStudyManager().searchInOrganization(organizationId, query, options, token);
//        if (studyDataResult.getNumResults() == 0) {
//            throw new CatalogException("Could not index catalog into solr. No studies found");
//        }
//
//        // Create solr collections if they don't exist
//        catalogSolrManager.createSolrCollections(CatalogSolrManager.FAMILY_SOLR_COLLECTION);
//
//        for (Study study : studyDataResult.getResults()) {
//            Map<String, Set<String>> studyAcls = SolrConverterUtil
//                    .parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
//            // We replace the current studyAcls for the parsed one
//            study.getAttributes().put("OPENCGA_ACL", studyAcls);
//
//            indexFamily(catalogSolrManager, study);
//        }
    }

//    private void indexFamily(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException {
//        logger.info("Indexing families of study {}", study.getFqn());
//
//        Query query = new Query();
//        QueryOptions familyQueryOptions = new QueryOptions()
//                .append(QueryOptions.INCLUDE, Arrays.asList(FamilyDBAdaptor.QueryParams.UUID.key(),
//                        FamilyDBAdaptor.QueryParams.CREATION_DATE.key(), FamilyDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
//                        FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), FamilyDBAdaptor.QueryParams.RELEASE.key(),
//                        FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key(),
//                        FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), FamilyDBAdaptor.QueryParams.EXPECTED_SIZE.key()))
//                .append(DBAdaptor.INCLUDE_ACLS, true)
//                .append(ParamConstants.FLATTEN_ANNOTATIONS, true);
//
//        catalogSolrManager.insertCatalogCollection(catalogManager.getFamilyManager().iterator(study.getFqn(), query,
//                familyQueryOptions, token), new CatalogFamilyToSolrFamilyConverter(study), CatalogSolrManager.FAMILY_SOLR_COLLECTION);
//    }
}

