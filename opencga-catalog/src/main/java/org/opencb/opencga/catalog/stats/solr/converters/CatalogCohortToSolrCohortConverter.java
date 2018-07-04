package org.opencb.opencga.catalog.stats.solr.converters;

import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.CohortSolrModel;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.Sample;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 03/07/18.
 */
public class CatalogCohortToSolrCohortConverter implements ComplexTypeConverter<Cohort, CohortSolrModel> {
    @Override
    public Cohort convertToDataModelType(CohortSolrModel object) {
        try {
            throw new Exception("Not supported operation!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public CohortSolrModel convertToStorageType(Cohort cohort) {
        CohortSolrModel cohortSolrModel = new CohortSolrModel();

        cohortSolrModel.setUid(cohort.getUid());
        cohortSolrModel.setType(cohort.getType().name());
        cohortSolrModel.setCreationDate(cohort.getCreationDate());
        cohortSolrModel.setStatus(cohort.getStatus().getName());
        if (cohort.getFamily() != null) {
            cohortSolrModel.setFamilyUuid(cohort.getFamily().getId());
        }

        cohortSolrModel.setSamplesUuid(populateSamples(cohort.getSamples()));
        cohortSolrModel.setRelease(cohort.getRelease());
        cohortSolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(cohort.getAnnotationSets()));


        return cohortSolrModel;
    }

    //****************Private*********************//

    private List<String> populateSamples(List<Sample> sampleList) {
        List<String> samplesUuid = new ArrayList<>();
        for (Sample sample : sampleList) {
            samplesUuid.add(sample.getId());
        }
        return samplesUuid;
    }
}
