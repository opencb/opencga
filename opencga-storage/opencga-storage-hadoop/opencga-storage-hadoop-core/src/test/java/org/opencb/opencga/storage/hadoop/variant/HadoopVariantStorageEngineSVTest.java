package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSVTest;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created on 26/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageEngineSVTest extends VariantStorageEngineSVTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @Override
    protected void loadFiles() throws Exception {
        super.loadFiles();
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Test
    public void checkSampleIndex() throws Exception {
        for (Variant variant : variantStorageEngine) {
            Set<String> samplesInVariant = new HashSet<>();
            for (String sample : metadataManager.getIndexedSamplesMap(studyMetadata.getId()).keySet()) {
                QueryOptions options = new QueryOptions(VariantHadoopDBAdaptor.NATIVE, false);
                VariantQueryResult<Variant> result
                        = variantStorageEngine.get(new Query(VariantQueryParam.SAMPLE.key(), sample).append(VariantQueryParam.ID.key(), variant), options);
                if (GenotypeClass.MAIN_ALT.test(variant.getStudies().get(0).getSample(sample).getData().get(0))) {
                    Assert.assertNotNull(result.first());
                    samplesInVariant.add(sample);
                } else {
                    Assert.assertNull(result.first());
                }
            }
            List<String> actualSampleNames = variantStorageEngine.getSampleData(variant.toString(), studyMetadata.getName(), new QueryOptions()).first().getSampleNames(studyMetadata.getName());
            Assert.assertEquals(samplesInVariant, new HashSet<>(actualSampleNames));
        }
    }

}
