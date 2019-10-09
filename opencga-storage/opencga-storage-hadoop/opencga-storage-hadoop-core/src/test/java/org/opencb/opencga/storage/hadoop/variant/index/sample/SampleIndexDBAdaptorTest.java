package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by jacobo on 21/03/19.
 */
public class SampleIndexDBAdaptorTest {

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

        SampleIndexQuery query = new SampleIndexQuery(Collections.emptyList(), "ST",
                Collections.singletonMap(sampleName, Collections.singletonList("0/1")), VariantQueryUtils.QueryOperation.AND);
        new SampleIndexDBAdaptor(new GenomeHelper(new Configuration()), new HBaseManager(new Configuration()), null, metadataManager).parse(query.forSample(sampleName), null, false);
    }


}