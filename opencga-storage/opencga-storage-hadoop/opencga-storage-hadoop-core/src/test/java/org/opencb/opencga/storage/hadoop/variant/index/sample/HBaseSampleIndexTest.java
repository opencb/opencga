package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexTest;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.annotation.SampleIndexAnnotationLoaderDriver;
import org.opencb.opencga.storage.hadoop.variant.index.sample.family.FamilyIndexDriver;
import org.opencb.opencga.storage.hadoop.variant.index.sample.file.SampleIndexDriver;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.stream.Collectors;

@Category(LongTests.class)
public class HBaseSampleIndexTest extends SampleIndexTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void load() throws Exception {
        super.load();

        VariantHbaseTestUtils.printVariants((VariantHadoopDBAdaptor) dbAdaptor, newOutputUri());
    }


    @Test
    public void regenerateSampleIndex() throws Exception {
        VariantHadoopDBAdaptor dbAdaptor = (VariantHadoopDBAdaptor) super.dbAdaptor;

        for (String study : studies) {
            int studyId = dbAdaptor.getMetadataManager().getStudyId(study);
            // Get the version with ALL samples indexed
            // This is a special case for STUDY, that has a sample index version with missing samples
            int version = sampleIndexDBAdaptor.getSchemaFactory()
                    .getSchema(studyId, dbAdaptor.getMetadataManager().getIndexedSamplesMap(studyId).keySet(), true, false).getVersion();
            String orig = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(studyId, version);
            String copy = orig + "_copy";

            dbAdaptor.getHBaseManager().createTableIfNeeded(copy, Bytes.toBytes(GenomeHelper.COLUMN_FAMILY),
                    Compression.Algorithm.NONE);

            ObjectMap options = new ObjectMap()
                    .append(SampleIndexDriver.SAMPLE_INDEX_VERSION, version)
                    .append(SampleIndexDriver.OUTPUT, copy)
                    .append(SampleIndexDriver.SAMPLES, "all");
            getMrExecutor().run(SampleIndexDriver.class, SampleIndexDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    Collections.emptySet(), options), "");

            getMrExecutor().run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    Collections.emptySet(), options), "");

            if (sampleNames.get(study).containsAll(trios.get(0).toList())) {
                options.put(FamilyIndexDriver.TRIOS, trios.stream().map(Trio::serialize).collect(Collectors.joining(";")));
                options.put(FamilyIndexDriver.OVERWRITE, true);
                getMrExecutor().run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                        dbAdaptor.getArchiveTableName(studyId),
                        dbAdaptor.getVariantTable(),
                        studyId,
                        Collections.emptySet(), options), "");
            } else if (study.equals(STUDY_NAME_3)) {
                options.put(FamilyIndexDriver.TRIOS, triosPlatinum.stream().map(Trio::serialize).collect(Collectors.joining(";")));
                options.put(FamilyIndexDriver.OVERWRITE, true);
                getMrExecutor().run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
                        dbAdaptor.getArchiveTableName(studyId),
                        dbAdaptor.getVariantTable(),
                        studyId,
                        Collections.emptySet(), options), "");
            }

            Connection c = dbAdaptor.getHBaseManager().getConnection();

            VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), studyId, copy);
            VariantHbaseTestUtils.printSampleIndexTable2(dbAdaptor, Paths.get(newOutputUri()), studyId, copy);

            assertEqualTables(c, orig, copy);
        }



//        VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), copy);

    }
}
