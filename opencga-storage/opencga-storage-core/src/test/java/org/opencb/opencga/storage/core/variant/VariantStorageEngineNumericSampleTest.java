package org.opencb.opencga.storage.core.variant;

import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class VariantStorageEngineNumericSampleTest extends VariantStorageBaseTest {

    public static final List<String> SAMPLES = Arrays.asList("SAMPLE_1", "1", "2", "4");

    @Test
    public void index() throws Exception {
        StudyMetadata studyMetadata = newStudyMetadata();
        VariantStorageEngine engine = getVariantStorageEngine();
        runDefaultETL(getResourceUri("variant-test-numeric-sample.vcf"), engine, studyMetadata,
                new ObjectMap()
                        .append(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), true)
        );
        VariantDBIterator iterator = engine.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), true), new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            StudyEntry study = variant.getStudy(STUDY_NAME);
            assertEquals(SAMPLES, study.getOrderedSamplesName());
            for (SampleEntry sample : study.getSamples()) {
                assertEquals(sample.getSampleId(), sample.getData().get(study.getSampleDataKeyPosition("NAME")));
            }
            assertEquals(study.getSamples(), engine.getSampleData(variant.toString(), study.getStudyId(), new QueryOptions()).first().getStudies().get(0).getSamples());
        }
    }

}
