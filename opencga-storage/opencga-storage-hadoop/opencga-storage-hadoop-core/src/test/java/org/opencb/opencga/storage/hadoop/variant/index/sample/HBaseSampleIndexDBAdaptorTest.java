package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.core.variant.index.sample.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.core.variant.index.sample.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.util.Collections;

/**
 * Created by jacobo on 21/03/19.
 */
@Category(ShortTests.class)
public class HBaseSampleIndexDBAdaptorTest {

    private VariantStorageMetadataManager metadataManager;
    private int studyId;

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        studyId = metadataManager.createStudy("ST").getId();
    }

    @Test
    public void testSampleIdFF() throws Exception {
        int sampleId = 0xFF;
        String sampleName = "FF";
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, sampleId, sampleName));

        SampleIndexQuery query = new SampleIndexQuery(SampleIndexSchema.defaultSampleIndexSchema(), Collections.emptyList(), 0, null, "ST",
                Collections.singletonMap(sampleName, Collections.singletonList("0/1")), Collections.emptySet(), null, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), new SampleAnnotationIndexQuery(SampleIndexSchema.defaultSampleIndexSchema()),
                Collections.emptySet(), null, false, VariantQueryUtils.QueryOperation.AND, null);
        new HBaseSampleIndexDBAdaptor(new HBaseManager(new Configuration()),
                new HBaseVariantTableNameGenerator("default", "my_dbname"), metadataManager)
                .parse(query.forSample(sampleName), null);
    }


}