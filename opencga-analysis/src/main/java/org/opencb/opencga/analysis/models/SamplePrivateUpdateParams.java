package org.opencb.opencga.analysis.models;

import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.ExternalSource;
import org.opencb.opencga.core.models.common.StatusParams;
import org.opencb.opencga.core.models.sample.*;

import java.util.List;
import java.util.Map;

public class SamplePrivateUpdateParams extends SampleUpdateParams {

    private SampleInternal internal;

    public SamplePrivateUpdateParams() {
    }

    public SamplePrivateUpdateParams(String id, String description, String creationDate, String modificationDate, String individualId,
                                     ExternalSource source, SampleProcessing processing, SampleCollection collection,
                                     SampleQualityControl qualityControl, SampleQualityControlStatus qualityControlStatus, Boolean somatic,
                                     List<Phenotype> phenotypes, List<AnnotationSet> annotationSets, Map<String, Object> attributes,
                                     StatusParams status, SampleInternal internal) {
        super(id, description, creationDate, modificationDate, individualId, source, processing, collection, qualityControl,
                qualityControlStatus, somatic, phenotypes, annotationSets, attributes, status);
        this.internal = internal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SamplePrivateUpdateParams{");
        sb.append("internal=").append(internal);
        sb.append('}');
        return sb.toString();
    }

    public SampleInternal getInternal() {
        return internal;
    }

    public SamplePrivateUpdateParams setInternal(SampleInternal internal) {
        this.internal = internal;
        return this;
    }
}
