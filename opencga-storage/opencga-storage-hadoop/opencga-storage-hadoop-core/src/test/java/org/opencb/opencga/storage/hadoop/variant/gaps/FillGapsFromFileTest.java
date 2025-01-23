package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.newOutputUri;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.newRootDir;


@Category(ShortTests.class)
public class FillGapsFromFileTest  {

    private VariantStorageMetadataManager vsmm;
    private VariantReaderUtils readerUtils;
    private int studyId;

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        vsmm = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        newRootDir(this.getClass().getSimpleName());

        readerUtils = new VariantReaderUtils(IOConnectorProvider.localDefault());

        studyId = vsmm.createStudy(VariantStorageBaseTest.STUDY_NAME).getId();
    }

    public URI indexFile(String fileName) throws StorageEngineException, IOException {
        URI uri = VariantStorageBaseTest.getResourceUri(fileName);
        List<String> sampleIds = readerUtils.readVariantFileMetadata(uri).getSampleIds();
        int fileId = vsmm.registerFile(studyId, fileName, sampleIds);
        vsmm.addIndexedFiles(studyId, Collections.singletonList(fileId));
        return uri;
    }

    @Test
    public void test() throws Exception {
        URI file1 = indexFile("gaps/file1.genome.vcf");
        URI file2 = indexFile("gaps/file2.genome.vcf");

        FillGapsFromFile task = new FillGapsFromFile(null, vsmm, readerUtils, new ObjectMap());
        URI result = task.fillGaps(VariantStorageBaseTest.STUDY_NAME, Arrays.asList(file1, file2), newOutputUri(), "0/0");
        System.out.println("result = " + result);
    }

    @Test
    public void testPlatinum() throws Exception {
        URI file1 = indexFile("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        URI file2 = indexFile("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");

        FillGapsFromFile task = new FillGapsFromFile(null, vsmm, readerUtils, new ObjectMap());
        task.fillGaps(VariantStorageBaseTest.STUDY_NAME, Arrays.asList(file1, file2), newOutputUri(), "0/0");
    }
}