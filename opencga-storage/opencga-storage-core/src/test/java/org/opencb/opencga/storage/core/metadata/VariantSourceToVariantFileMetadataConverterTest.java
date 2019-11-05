package org.opencb.opencga.storage.core.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;

import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Created on 07/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSourceToVariantFileMetadataConverterTest extends VariantStorageBaseTest implements DummyVariantStorageTest {

    @Test
    public void testConvert() throws Exception {

        // Transform smallInputFile to get the expected meta file with stats
        StoragePipelineResult storagePipelineResult = runETL(variantStorageEngine, smallInputUri, newOutputUri(), new ObjectMap(VariantStorageOptions.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true), true, true, false);
        VariantFileMetadata expectedFileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(storagePipelineResult.getTransformResult());

        // Read and convert the legacy metadata file
        InputStream resource = new GZIPInputStream(getClass().getResourceAsStream("/variant-test-file.vcf.gz.file_legacy.json.gz"));
        org.opencb.biodata.models.variant.avro.legacy.VariantSource legacy =
                new ObjectMapper().readValue(resource, org.opencb.biodata.models.variant.avro.legacy.VariantSource.class);
        VariantFileMetadata convertedFileMetadata = new VariantSourceToVariantFileMetadataConverter().convert(legacy);

        // Impossible to get StdDev from legacy VariantSource
        expectedFileMetadata.getStats().setStdDevQuality(0);

        assertEquals(expectedFileMetadata, convertedFileMetadata);
    }

}