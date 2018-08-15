package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.CohortSolrModel;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.Study;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wasim on 03/07/18.
 */
public class CatalogCohortToSolrCohortConverter implements ComplexTypeConverter<Cohort, CohortSolrModel> {

    private Study study;

    public CatalogCohortToSolrCohortConverter(Study study) {
        this.study = study;
    }

    @Override
    public Cohort convertToDataModelType(CohortSolrModel object) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public CohortSolrModel convertToStorageType(Cohort cohort) {
        CohortSolrModel cohortSolrModel = new CohortSolrModel();

        cohortSolrModel.setUid(cohort.getUid());
        cohortSolrModel.setStudyId(study.getFqn().replace(":", "__"));
        cohortSolrModel.setType(cohort.getType().name());
        cohortSolrModel.setCreationDate(cohort.getCreationDate());
        cohortSolrModel.setStatus(cohort.getStatus().getName());

        if (cohort.getSamples() != null) {
            cohortSolrModel.setSamples(cohort.getSamples().size());
        }

        cohortSolrModel.setRelease(cohort.getRelease());
        cohortSolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(cohort.getAnnotationSets()));

        // Extract the permissions
        Map<String, Set<String>> cohortAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) cohort.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>) study.getAttributes().get("OPENCGA_ACL"), cohortAcl,
                        "COHORT");
        cohortSolrModel.setAcl(effectivePermissions);

        return cohortSolrModel;
    }
}
