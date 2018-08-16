package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.Study;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSampleToSolrSampleConverter implements ComplexTypeConverter<Sample, SampleSolrModel> {

    private Study study;

    public CatalogSampleToSolrSampleConverter(Study study) {
        this.study = study;
    }

    @Override
    public Sample convertToDataModelType(SampleSolrModel sampleSolrModel) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public SampleSolrModel convertToStorageType(Sample sample) {

        SampleSolrModel sampleSolrModel = new SampleSolrModel();

        sampleSolrModel.setUid(sample.getUid());
        sampleSolrModel.setSource(sample.getSource());
        sampleSolrModel.setStudyId(study.getFqn().replace(":", "__"));

        sampleSolrModel.setRelease(sample.getRelease());
        sampleSolrModel.setVersion(sample.getVersion());

        Date date = TimeUtils.toDate(sample.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        sampleSolrModel.setCreationYear(localDate.getYear());
        sampleSolrModel.setCreationMonth(localDate.getMonth().toString());
        sampleSolrModel.setCreationDay(localDate.getDayOfMonth());
        sampleSolrModel.setCreationDayOfWeek(localDate.getDayOfWeek().toString());
        sampleSolrModel.setStatus(sample.getStatus().getName());

        sampleSolrModel.setStatus(sample.getStatus().getName());
        sampleSolrModel.setType(sample.getType());
        sampleSolrModel.setSomatic(sample.isSomatic());

        if (sample.getPhenotypes() != null) {
            sampleSolrModel.setPhenotypes(SolrConverterUtil.populatePhenotypes(sample.getPhenotypes()));
        }
        if (sample.getAnnotationSets() != null) {
            sampleSolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(sample.getAnnotationSets()));
        }

        // Extract the permissions
        Map<String, Set<String>> sampleAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) sample.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>) study.getAttributes().get("OPENCGA_ACL"), sampleAcl,
                        "SAMPLE");
        sampleSolrModel.setAcl(effectivePermissions);

        return sampleSolrModel;
    }

}
