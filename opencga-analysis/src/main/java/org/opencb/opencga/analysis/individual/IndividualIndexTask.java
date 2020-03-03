package org.opencb.opencga.analysis.individual;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogIndividualToSolrIndividualConverter;
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

@Tool(id = IndividualIndexTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION,
        description = "Index Individual entries in Solr.")
public class IndividualIndexTask extends OpenCgaTool {

    public final static String ID = "individual-secondary-index";

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

            indexIndividual(catalogSolrManager, study);
        }

    }

    private void indexIndividual(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException {
        logger.info("Indexing individuals of study {}", study.getFqn());

        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryOptions individualQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(IndividualDBAdaptor.QueryParams.UUID.key(),
                        IndividualDBAdaptor.QueryParams.FATHER_UID.key(), IndividualDBAdaptor.QueryParams.MOTHER_UID.key(),
                        IndividualDBAdaptor.QueryParams.MULTIPLES.key(), IndividualDBAdaptor.QueryParams.SEX.key(),
                        IndividualDBAdaptor.QueryParams.ETHNICITY.key(), IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(),
                        IndividualDBAdaptor.QueryParams.RELEASE.key(), IndividualDBAdaptor.QueryParams.CREATION_DATE.key(),
                        IndividualDBAdaptor.QueryParams.VERSION.key(),
                        IndividualDBAdaptor.QueryParams.INTERNAL_STATUS.key(), IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(),
                        IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                        IndividualDBAdaptor.QueryParams.SAMPLE_UIDS.key(), IndividualDBAdaptor.QueryParams.PARENTAL_CONSANGUINITY.key(),
                        IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(), IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true)
                .append(Constants.FLATTENED_ANNOTATIONS, true);

        catalogSolrManager.insertCatalogCollection(catalogManager.getIndividualManager().iterator(study.getFqn(), query,
                individualQueryOptions, token), new CatalogIndividualToSolrIndividualConverter(study),
                CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION);
    }
}
