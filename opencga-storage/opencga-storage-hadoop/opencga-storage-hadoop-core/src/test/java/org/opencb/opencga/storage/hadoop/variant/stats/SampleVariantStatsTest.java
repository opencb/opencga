package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SampleVariantStatsTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private static boolean loaded = false;
    private static String study = "study";
    private static String father = "NA12877";
    private static String mother = "NA12878";
    private static String child = "NA12879";  // Maybe this is not accurate, but works file for the example

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();
    private int studyId;

    @Before
    public void before() throws Exception {
        if (!loaded) {
            loaded = true;
            HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
            URI outputUri = newOutputUri();

            ObjectMap params = new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), false)
                    .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                    .append(VariantStorageEngine.Options.STUDY.key(), study);
            runETL(variantStorageEngine, getPlatinumFile(12877), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12878), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12879), outputUri, params, true, true, true);




            List<String> family = Arrays.asList(father, mother, child);
            VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
            studyId = mm.getStudyId(study);
            mm.updateSampleMetadata(studyId, mm.getSampleId(studyId, child), sampleMetadata -> {
                sampleMetadata.setFather(mm.getSampleId(studyId, father));
                sampleMetadata.setMother(mm.getSampleId(studyId, mother));
                return sampleMetadata;
            });

            variantStorageEngine.annotate(new Query(), new ObjectMap());
            variantStorageEngine.fillGaps(study, family, new ObjectMap());


            VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
        }
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Test
    public void test() throws Exception {

        HadoopVariantStorageEngine engine = getVariantStorageEngine();
        ObjectMap params = new ObjectMap();
        params.put(SampleVariantStatsDriver.SAMPLES, "all");

        getMrExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(null, engine.getVariantTableName(), 1, null, params), params);


        Iterator<SampleMetadata> it = engine.getMetadataManager().sampleMetadataIterator(studyId);
        while (it.hasNext()) {
            SampleMetadata sample = it.next();
            System.out.println(sample.getName());
            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(sample.getStats()));
        }
    }
}