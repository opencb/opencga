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

package org.opencb.opencga.analysis.file;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFileToSolrFileConverter;
import org.opencb.opencga.catalog.stats.solr.converters.SolrConverterUtil;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tool(id = FileIndexTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION, description = "Index File entries in Solr.")
public class FileIndexTask extends OpenCgaTool {

    public final static String ID = "file-secondary-index";

    private CatalogSolrManager catalogSolrManager;

    @Override
    protected void check() throws Exception {
        catalogSolrManager = new CatalogSolrManager(this.catalogManager);
//        catalogSolrManager.existsCollection()
//        if (!catalogSolrManager.isAlive(configuration.getDatabasePrefix() + CatalogSolrManager.FILE_SOLR_COLLECTION)) {
//            throw new ToolException("Solr not found");
//        }
    }

    @Override
    protected void run() throws Exception {
        // Get all the studies
        Query query = new Query();
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                        StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.FQN.key(),
                        StudyDBAdaptor.QueryParams.VARIABLE_SET.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true);
        OpenCGAResult<Study> studyDataResult = catalogManager.getStudyManager().search(query, options, token);
        if (studyDataResult.getNumResults() == 0) {
            throw new CatalogException("Could not index catalog into solr. No studies found");
        }

        // Create solr collections if they don't exist
        catalogSolrManager.createSolrCollections(CatalogSolrManager.FILE_SOLR_COLLECTION);

        for (Study study : studyDataResult.getResults()) {
            Map<String, Set<String>> studyAcls = SolrConverterUtil
                    .parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
            // We replace the current studyAcls for the parsed one
            study.getAttributes().put("OPENCGA_ACL", studyAcls);

            indexFile(catalogSolrManager, study);
        }

    }

    private void indexFile(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException {
        logger.info("Indexing files of study {}", study.getFqn());

        Query query = new Query();
        QueryOptions fileQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.UUID.key(),
                        FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.TYPE.key(),
                        FileDBAdaptor.QueryParams.FORMAT.key(),
                        FileDBAdaptor.QueryParams.CREATION_DATE.key(), FileDBAdaptor.QueryParams.BIOFORMAT.key(),
                        FileDBAdaptor.QueryParams.RELEASE.key(), FileDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                        FileDBAdaptor.QueryParams.EXTERNAL.key(), FileDBAdaptor.QueryParams.SIZE.key(),
                        FileDBAdaptor.QueryParams.SOFTWARE.key(), FileDBAdaptor.QueryParams.EXPERIMENT.key(),
                        FileDBAdaptor.QueryParams.RELATED_FILES.key(), FileDBAdaptor.QueryParams.SAMPLE_IDS.key(),
                        FileDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(ParamConstants.FLATTEN_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(catalogManager.getFileManager().iterator(study.getFqn(), query,
                fileQueryOptions, token), new CatalogFileToSolrFileConverter(study), CatalogSolrManager.FILE_SOLR_COLLECTION);
    }
}
