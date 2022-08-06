package org.opencb.opencga.storage.core.variant;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Ignore
public abstract class VariantStorageEngineSampleRenamingTest extends VariantStorageBaseTest {

    @Test
    public void test() throws Exception {
        runETL(getVariantStorageEngine(), getResourceUri("variant-test-sample-mapping.vcf"), STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        VariantStorageMetadataManager mm = getVariantStorageEngine().getMetadataManager();
        int studyId = mm.getStudyId(STUDY_NAME);
        FileMetadata fileMetadata = mm.getFileMetadata(studyId, "variant-test-sample-mapping.vcf");
        Assert.assertEquals(3, fileMetadata.getSamples().size());

        List<String> actualSampleNames = new ArrayList<>(3);
        for (Integer sample : fileMetadata.getSamples()) {
            actualSampleNames.add(mm.getSampleName(studyId, sample));
        }
        Assert.assertEquals(Arrays.asList("sample_tumor", "sample_normal" , "sample_other"), actualSampleNames);


    }

}
