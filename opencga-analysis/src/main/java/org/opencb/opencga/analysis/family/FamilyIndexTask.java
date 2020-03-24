package org.opencb.opencga.analysis.family;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFamilyToSolrFamilyConverter;
import org.opencb.opencga.catalog.stats.solr.converters.SolrConverterUtil;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        // Get all the studies
        Query query = new Query();
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                        StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.FQN.key(),
                        StudyDBAdaptor.QueryParams.VARIABLE_SET.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true);
        OpenCGAResult<Study> studyDataResult = catalogManager.getStudyManager().get(query, options, token);
        if (studyDataResult.getNumResults() == 0) {
            throw new CatalogException("Could not index catalog into solr. No studies found");
        }

        // Create solr collections if they don't exist
        catalogSolrManager.createSolrCollections();

        for (Study study : studyDataResult.getResults()) {
            Map<String, Set<String>> studyAcls = SolrConverterUtil
                    .parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
            // We replace the current studyAcls for the parsed one
            study.getAttributes().put("OPENCGA_ACL", studyAcls);

            indexFamily(catalogSolrManager, study);
        }

    }

    private void indexFamily(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException {
        logger.info("Indexing families of study {}", study.getFqn());

        Query query = new Query();
        QueryOptions familyQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(FamilyDBAdaptor.QueryParams.UUID.key(),
                        FamilyDBAdaptor.QueryParams.CREATION_DATE.key(), FamilyDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                        FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), FamilyDBAdaptor.QueryParams.RELEASE.key(),
                        FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key(),
                        FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), FamilyDBAdaptor.QueryParams.EXPECTED_SIZE.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(catalogManager.getFamilyManager().iterator(study.getFqn(), query,
                familyQueryOptions, token), new CatalogFamilyToSolrFamilyConverter(study), CatalogSolrManager.FAMILY_SOLR_COLLECTION);
    }
}

