package org.opencb.opencga.analysis.models;

import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.OntologyTermAnnotation;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.core.models.clinical.ClinicalIdentifier;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.individual.*;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;

import java.util.List;
import java.util.Map;

public class IndividualPrivateUpdateParams extends IndividualUpdateParams {

    private IndividualInternal internal;

    public IndividualPrivateUpdateParams() {
    }

    public IndividualPrivateUpdateParams(String id, String name, IndividualReferenceParam father, IndividualReferenceParam mother,
                                         String creationDate, String modificationDate, Boolean parentalConsanguinity, Location location,
                                         SexOntologyTermAnnotation sex, OntologyTermAnnotation ethnicity, IndividualPopulation population,
                                         String dateOfBirth, IndividualProperty.KaryotypicSex karyotypicSex,
                                         IndividualProperty.LifeStatus lifeStatus, List<SampleReferenceParam> samples,
                                         List<AnnotationSet> annotationSets, List<Phenotype> phenotypes, List<Disorder> disorders,
                                         StatusParams status, IndividualQualityControl qualityControl, Map<String, Object> attributes,
                                         List<ClinicalIdentifier> identifiers,  IndividualInternal internal) {
        super(id, name, father, mother, creationDate, modificationDate, parentalConsanguinity, location, sex, ethnicity, population,
                dateOfBirth, karyotypicSex, lifeStatus, samples, annotationSets, phenotypes, disorders, status, qualityControl, identifiers,
                attributes);
        this.internal = internal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualPrivateUpdateParams{");
        sb.append("internal=").append(internal);
        sb.append('}');
        return sb.toString();
    }

    public IndividualInternal getInternal() {
        return internal;
    }

    public IndividualPrivateUpdateParams setInternal(IndividualInternal internal) {
        this.internal = internal;
        return this;
    }
}
