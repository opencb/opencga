package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.newOutputUri;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.newRootDir;


@Category(ShortTests.class)
public class FillGapsFromFileTest  {

    private VariantStorageMetadataManager vsmm;
    private VariantReaderUtils readerUtils;
    private int studyId;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        vsmm = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        newRootDir(this.getClass().getSimpleName());

        readerUtils = new VariantReaderUtils(IOConnectorProvider.localDefault());

        studyId = vsmm.createStudy(VariantStorageBaseTest.STUDY_NAME, ParamConstants.CELLBASE_VERSION).getId();
    }

    public URI indexFile(String fileName) throws StorageEngineException, IOException {
        URI uri = VariantStorageBaseTest.getResourceUri(fileName);
        VariantFileMetadata variantFileMetadata = readerUtils.readVariantFileMetadata(uri);
        int fileId = vsmm.registerFile(studyId, variantFileMetadata);
        variantFileMetadata.setId(String.valueOf(fileId));
        vsmm.updateVariantFileMetadata(studyId, variantFileMetadata);
        vsmm.addIndexedFiles(studyId, Collections.singletonList(fileId));
        return uri;
    }

    @Test
    public void test() throws Exception {
        URI file1 = indexFile("gaps/file1.genome.vcf");
        URI file2 = indexFile("gaps/file2.genome.vcf");

        FillGapsFromFile task = new FillGapsFromFile(null, vsmm, readerUtils, new ObjectMap());
        Path result = task.fillGaps(VariantStorageBaseTest.STUDY_NAME, Arrays.asList(file1, file2), newOutputUri(), "0/0");
        printResult(result);
    }

    @Test
    public void testUnorderedChromosomes() throws Exception {
        URI file1 = indexFile("gaps_unordered_chrs/file1.genome.vcf");
        URI file2 = indexFile("gaps_unordered_chrs/file2.genome.vcf");

        FillGapsFromFile task = new FillGapsFromFile(null, vsmm, readerUtils, new ObjectMap());
        task.setMaxBufferSize(10);
        thrown.expectMessage("Chromosome \"2\" already processed!");
        task.fillGaps(VariantStorageBaseTest.STUDY_NAME, Arrays.asList(file1, file2), newOutputUri(), "0/0");
    }

    @Test
    public void testUnorderedChromosomesWithinLimit() throws Exception {
        URI file1 = indexFile("gaps_unordered_chrs/file1.genome.vcf");
        URI file2 = indexFile("gaps_unordered_chrs/file2.genome.vcf");

        FillGapsFromFile task = new FillGapsFromFile(null, vsmm, readerUtils, new ObjectMap());
        task.setMaxBufferSize(1000);
        Path result = task.fillGaps(VariantStorageBaseTest.STUDY_NAME, Arrays.asList(file1, file2), newOutputUri(), "0/0");
        printResult(result);
    }

    @Test
    public void testPlatinum() throws Exception {
        URI file1 = indexFile("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        URI file2 = indexFile("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");

        FillGapsFromFile task = new FillGapsFromFile(null, vsmm, readerUtils, new ObjectMap());
        Path result = task.fillGaps(VariantStorageBaseTest.STUDY_NAME, Arrays.asList(file1, file2), newOutputUri(), "0/0");

        printResult(result);
    }

    private static void printResult(Path result) throws Exception {
        try (CloseableIterator<Put> iterator = FillGapsFromFile.putProtoIterator(result)) {
            while (iterator.hasNext()) {
                Put put = iterator.next();
                StringBuilder sb = new StringBuilder();
                sb.append(VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(put.getRow(), null, null))
                        .append(" -> ");
                put.getFamilyCellMap().get(GenomeHelper.COLUMN_FAMILY_BYTES).forEach(cell -> {
                    String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    sb.append(column)
                            .append(":");
                    if (column.endsWith(VariantPhoenixSchema.SAMPLE_DATA_SUFIX) || column.endsWith(VariantPhoenixSchema.FILE_SUFIX)) {
                        sb.append(PVarcharArray.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    } else {
                        sb.append(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                    sb.append(" , ");
                });
                System.out.println(sb);
            }

        }
    }
}