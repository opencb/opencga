package org.opencb.opencga.core.models.individual;

import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.OntologyTermAnnotation;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.InternalQualityControl;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.family.FamilyQualityControl;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;

import java.util.List;
import java.util.Map;

public class IndividualInternalUpdateParams extends IndividualUpdateParams {

    private IndividualQualityControl qualityControl;
    private InternalQualityControl internal;

    public IndividualInternalUpdateParams() {
        super();
    }

    public IndividualInternalUpdateParams(IndividualQualityControl qualityControl, InternalQualityControl internal) {
        this.qualityControl = qualityControl;
        this.internal = internal;
    }

    public IndividualInternalUpdateParams(String id, String name, IndividualReferenceParam father, IndividualReferenceParam mother,
                                          String creationDate, String modificationDate, Boolean parentalConsanguinity,
                                          Location location, SexOntologyTermAnnotation sex, OntologyTermAnnotation ethnicity,
                                          IndividualPopulation population, String dateOfBirth,
                                          IndividualProperty.KaryotypicSex karyotypicSex, IndividualProperty.LifeStatus lifeStatus,
                                          List<SampleReferenceParam> samples, List<AnnotationSet> annotationSets,
                                          List<Phenotype> phenotypes, List<Disorder> disorders, StatusParams status,
                                          Map<String, Object> attributes, IndividualQualityControl qualityControl,
                                          InternalQualityControl internal) {
        super(id, name, father, mother, creationDate, modificationDate, parentalConsanguinity, location, sex, ethnicity, population,
                dateOfBirth, karyotypicSex, lifeStatus, samples, annotationSets, phenotypes, disorders, status, attributes);
        this.qualityControl = qualityControl;
        this.internal = internal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualInternalUpdateParams{");
        sb.append("qualityControl=").append(qualityControl);
        sb.append(", internal=").append(internal);
        sb.append('}');
        return sb.toString();
    }

    public IndividualQualityControl getQualityControl() {
        return qualityControl;
    }

    public IndividualInternalUpdateParams setQualityControl(IndividualQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }

    public InternalQualityControl getInternal() {
        return internal;
    }

    public IndividualInternalUpdateParams setInternal(InternalQualityControl internal) {
        this.internal = internal;
        return this;
    }
}
