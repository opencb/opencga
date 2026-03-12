package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.gaps.FillGapsTest;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;

import java.net.URI;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.assertArrayEquals;

@Category(LongTests.class)
public class HadoopFillGapsTest extends FillGapsTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @Override
    protected void logVariansStatus(VariantDBAdaptor dbAdaptor) throws Exception {
        VariantHbaseTestUtils.printVariants((VariantHadoopDBAdaptor) dbAdaptor, newOutputUri());
    }

    @Override
    protected void logVariansStatus(StudyMetadata studyMetadata, VariantDBAdaptor dbAdaptor, URI outputUri) throws Exception {
        VariantHbaseTestUtils.printVariants(studyMetadata, (VariantHadoopDBAdaptor) dbAdaptor, outputUri);
    }

    @Test
    public void testFillGapsPlatinumFilesMultiFileBatch() throws Exception {
        StudyMetadata studyMetadata = loadPlatinum(new ObjectMap(HadoopVariantStorageOptions.ARCHIVE_FILE_BATCH_SIZE.key(), 2)
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC), 12877, 12880);
        testFillGapsPlatinumFiles(studyMetadata, true);
    }


    @Test
    public void testFillMissingFilterNonRef() throws Exception {
        testFillMissingFilterNonRef(new ObjectMap()
                .append(VariantStorageOptions.LOAD_ARCHIVE.key(), YesNoAuto.YES)
                .append(HadoopVariantStorageOptions.ARCHIVE_NON_REF_FILTER.key(), "FORMAT:DP<6"));
    }

    @Override
    protected void checkInputValuesAreUnmodified(String aggregatedStudy, String referenceStudy) throws Exception {
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();

        int studyId1 = dbAdaptor.getMetadataManager().getStudyId(aggregatedStudy);
        int studyId2 = dbAdaptor.getMetadataManager().getStudyId(referenceStudy);

        dbAdaptor.getHBaseManager().act(dbAdaptor.getVariantTable(), table -> {
            table.getScanner(new Scan()).iterator().forEachRemaining(r -> {
                Variant variant = VariantPhoenixKeyFactory.extractVariantFromResult(r);

                NavigableMap<byte[], byte[]> cells = r.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES);
                for (Map.Entry<byte[], byte[]> entry : cells.entrySet()) {
                    String columnKey = Bytes.toString(entry.getKey());
                    Integer studyId = VariantPhoenixSchema.extractStudyId(columnKey, false);
                    if (studyId != null && studyId == studyId2) {
                        String otherColumnKey = columnKey.replaceFirst(VariantPhoenixSchema.buildStudyColumnsPrefix(studyId2),
                                VariantPhoenixSchema.buildStudyColumnsPrefix(studyId1));
                        byte[] thisCell = entry.getValue();
                        byte[] otherCell = cells.get(Bytes.toBytes(otherColumnKey));
                        assertArrayEquals(variant.toString() + " study1ColumnKey " + otherColumnKey + ", study2ColumnKey " + columnKey,
                                thisCell, otherCell);
                    }
                }
            });
        });
    }

    @Test
    public void testFillMissingPlatinumFiles() throws Exception {
        testFillMissingPlatinumFiles(new ObjectMap()
                .append(VariantStorageOptions.LOAD_ARCHIVE.key(), YesNoAuto.YES)
                .append(HadoopVariantStorageOptions.ARCHIVE_FILE_BATCH_SIZE.key(), 2));
    }
}
