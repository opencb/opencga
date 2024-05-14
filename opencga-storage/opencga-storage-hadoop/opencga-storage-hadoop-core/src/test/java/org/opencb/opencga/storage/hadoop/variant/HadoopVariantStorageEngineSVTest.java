package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSVTest;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Created on 26/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(LongTests.class)
public class HadoopVariantStorageEngineSVTest extends VariantStorageEngineSVTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void loadFiles() throws Exception {
        super.loadFiles();
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Test
    public void checkSampleIndex() throws Exception {
        for (Variant variant : variantStorageEngine.iterable(new VariantQuery()
                        .includeSampleAll()
                        .includeSampleId(true)
                , new QueryOptions())) {
            Set<String> samplesInVariant = new HashSet<>();
            for (Map.Entry<String, Integer> entry : metadataManager.getStudies().entrySet()) {
                String studyName = entry.getKey();
                Integer studyId = entry.getValue();
                StudyEntry studyEntry = variant.getStudy(studyName);
                if (studyEntry != null) {
                    for (String sample : metadataManager.getIndexedSamplesMap(studyId).keySet()) {
                        QueryOptions options = new QueryOptions(VariantHadoopDBAdaptor.NATIVE, false);
                        VariantQueryResult<Variant> result = variantStorageEngine.get(new VariantQuery()
                                .study(studyName)
                                .id(variant.toString())
                                .sample(sample), options);
                        String genotype = studyEntry.getSample(sample).getData().get(0);
                        String message = "Study=" + studyName + " Sample=" + sample + " with GT=" + genotype + " in variant=" + variant;
                        if (GenotypeClass.MAIN_ALT.test(genotype)) {
                            Assert.assertNotNull(message, result.first());
                            samplesInVariant.add(sample);
                        } else {
                            Assert.assertNull(message, result.first());
                        }
                    }
                    logger.info("Variant " + variant + " with samples " + samplesInVariant);
                    logger.info("Query variant " + variant + " in study " + studyName + " from sampleData");
                    Variant sampleDataVariant = variantStorageEngine.getSampleData(variant.toString(), studyName, new QueryOptions()).first();
                    List<String> actualSampleNames = sampleDataVariant.getSampleNames(studyName);
                    logger.info("Variant " + variant + " with actual samples " + actualSampleNames);
                    Assert.assertEquals(samplesInVariant, new HashSet<>(actualSampleNames));
                }
            }
        }
    }

    @Test
    public void checkPipelineResult() {
        assertEquals(pipelineResult1.getLoadStats().getInt("expectedVariants"), pipelineResult1.getLoadStats().getInt("loadedVariants"));
        assertEquals(pipelineResult2.getLoadStats().getInt("expectedVariants"), pipelineResult2.getLoadStats().getInt("loadedVariants"));
    }
}
