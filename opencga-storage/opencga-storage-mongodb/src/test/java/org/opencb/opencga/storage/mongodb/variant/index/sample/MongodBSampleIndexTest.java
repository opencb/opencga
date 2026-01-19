package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexTest;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;

@Category(LongTests.class)
public class MongodBSampleIndexTest extends SampleIndexTest implements MongoDBVariantStorageTest {


    public void printSampleIndexContents(String study) throws Exception {
        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) super.dbAdaptor;
        int studyId = dbAdaptor.getMetadataManager().getStudyId(study);

        dbAdaptor.getMongoDataStoreManager();
        for (Integer sampleId : metadataManager.getIndexedSamples(studyId)) {
            String sampleName = metadataManager.getSampleName(studyId, sampleId);

            Query query = new VariantQuery().study(study).sample(sampleName);
            SampleIndexSchema schema = sampleIndexDBAdaptor.parseSampleIndexQuery(query).getSchema();
            try (CloseableIterator<SampleIndexVariant> it = sampleIndexDBAdaptor.indexVariantIterator(study, sampleName)) {
                while (it.hasNext()) {
                    SampleIndexVariant variant = it.next();
                    System.out.println("Sample: " + sampleName + "\t" + variant.toString(schema));
                }
            }
        }
    }

    @Test
    public void regenerateSampleIndex() throws Exception {
        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) super.dbAdaptor;

        for (String study : studies) {
            int studyId = dbAdaptor.getMetadataManager().getStudyId(study);
            // Get the version with ALL samples indexed
            // This is a special case for STUDY, that has a sample index version with missing samples
            int version = sampleIndexDBAdaptor.getSchemaFactory()
                    .getSchema(studyId, dbAdaptor.getMetadataManager().getIndexedSamplesMap(studyId).keySet(), true, false).getVersion();
            dbAdaptor.getMongoDataStoreManager();
//            String orig = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(studyId, version);
//            String copy = orig + "_copy";
//
//            dbAdaptor.getHBaseManager().createTableIfNeeded(copy, Bytes.toBytes(GenomeHelper.COLUMN_FAMILY),
//                    Compression.Algorithm.NONE);
//
//            ObjectMap options = new ObjectMap()
//                    .append(SampleIndexDriver.SAMPLE_INDEX_VERSION, version)
//                    .append(SampleIndexDriver.OUTPUT, copy)
//                    .append(SampleIndexDriver.SAMPLES, "all");
//            getMrExecutor().run(SampleIndexDriver.class, SampleIndexDriver.buildArgs(
//                    dbAdaptor.getArchiveTableName(studyId),
//                    dbAdaptor.getVariantTable(),
//                    studyId,
//                    Collections.emptySet(), options), "");
//
//            getMrExecutor().run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
//                    dbAdaptor.getArchiveTableName(studyId),
//                    dbAdaptor.getVariantTable(),
//                    studyId,
//                    Collections.emptySet(), options), "");
//
//            if (sampleNames.get(study).containsAll(trios.get(0).toList())) {
//                options.put(FamilyIndexDriver.TRIOS, trios.stream().map(Trio::serialize).collect(Collectors.joining(";")));
//                options.put(FamilyIndexDriver.OVERWRITE, true);
//                getMrExecutor().run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
//                        dbAdaptor.getArchiveTableName(studyId),
//                        dbAdaptor.getVariantTable(),
//                        studyId,
//                        Collections.emptySet(), options), "");
//            } else if (study.equals(STUDY_NAME_3)) {
//                options.put(FamilyIndexDriver.TRIOS, triosPlatinum.stream().map(Trio::serialize).collect(Collectors.joining(";")));
//                options.put(FamilyIndexDriver.OVERWRITE, true);
//                getMrExecutor().run(FamilyIndexDriver.class, FamilyIndexDriver.buildArgs(
//                        dbAdaptor.getArchiveTableName(studyId),
//                        dbAdaptor.getVariantTable(),
//                        studyId,
//                        Collections.emptySet(), options), "");
//            }
//
//            Connection c = dbAdaptor.getHBaseManager().getConnection();
//
//            VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), studyId, copy);
//            VariantHbaseTestUtils.printSampleIndexTable2(dbAdaptor, Paths.get(newOutputUri()), studyId, copy);
//
//            assertEqualTables(c, orig, copy);
        }



//        VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), copy);

    }

}
