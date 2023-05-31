/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfSliceToVariantListConverter;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.TARGET_VARIANT_TYPE_SET;

/**
 * Created on 15/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class VariantHadoopStoragePipelineTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private VariantHadoopDBAdaptor dbAdaptor;
    private static StudyMetadata studyMetadata;
    private static VariantFileMetadata fileMetadata;
    private static StoragePipelineResult etlResult = null;

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private DataResult<Variant> allVariantsQueryResult;

    @BeforeClass
    public static void beforeClass() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = externalResource.getVariantStorageEngine();
        externalResource.clearDB(variantStorageManager.getDBName());

//        URI inputUri = VariantStorageBaseTest.getResourceUri("sample1.genome.vcf");
        URI inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
//            URI inputUri = VariantStorageManagerTestUtils.getResourceUri("variant-test-file.vcf.gz");

        try {
            studyMetadata = VariantStorageBaseTest.newStudyMetadata();
            etlResult = VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageManager, studyMetadata,
                    new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                            .append(VariantStorageOptions.ANNOTATE.key(), true)
                            .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
            );

            fileMetadata = variantStorageManager.getVariantReaderUtils().readVariantFileMetadata(etlResult.getTransformResult());
            VariantSetStats stats = fileMetadata.getStats();
            Assert.assertNotNull(stats);
        } finally {
            try (VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor()) {
                VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
//                VariantHbaseTestUtils.printVariantsFromVariantsTable(dbAdaptor);
//                VariantHbaseTestUtils.printVariantsFromArchiveTable(dbAdaptor, studyConfiguration);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Before
    @Override
    public void before() throws Exception {
        dbAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor();

        if (allVariantsQueryResult == null) {
            allVariantsQueryResult = dbAdaptor.get(new Query(), new QueryOptions());
        }
    }

    @After
    public void tearDown() throws Exception {
        dbAdaptor.close();
    }

    @Test
    public void testConnection() throws StorageEngineException {
        variantStorageEngine.testConnection();
    }

    @Test
    public void queryVariantTable() {
        System.out.println("Query from Variant table");
        VariantDBIterator iterator = dbAdaptor.iterator(
                new Query(VariantQueryParam.STUDY.key(), studyMetadata.getId()),
                new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            System.out.println("Phoenix variant = " + variant);
        }
        System.out.println("End query from Analysis table");
    }

    @Test
    public void countVariants() {
        long totalCount = dbAdaptor.count(new Query()).first();

        long count = TARGET_VARIANT_TYPE_SET.stream()
                .mapToLong(type -> fileMetadata.getStats().getTypeCount().getOrDefault(type.name(), 0L))
                .sum();
//        count  -= 1; // Deletion is in conflict with other variant: 1:10403:ACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAAC:A
        assertEquals(count, totalCount);
    }

    @Test
    public void getVariantByGene() throws StorageEngineException {

        // Group by Gene
        HashMap<String, Long> genesCount = new HashMap<>();
        for (Variant variant : allVariantsQueryResult.getResults()) {
            HashSet<String> genesInVariant = new HashSet<>();
            for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                String geneName = consequenceType.getGeneName();
                if (geneName != null) {
                    genesInVariant.add(geneName);
                }
                geneName = consequenceType.getGeneId();
                if (geneName != null) {
                    genesInVariant.add(geneName);
                }
            }
            for (String geneName : genesInVariant) {
                genesCount.put(geneName, genesCount.getOrDefault(geneName, 0L) + 1);
            }
        }
        System.out.println("genesCount = " + genesCount);
        System.out.println("genes = " + genesCount.size());

        int maxGeneQueries = 30;
        //Count for each gene
        for (Map.Entry<String, Long> entry : genesCount.entrySet()) {
            if (maxGeneQueries-- == 0) {
                break;
            }
            System.out.println("Gene " + entry.getKey() + " in " + entry.getValue() + " variants");
            DataResult<Long> queryResult = variantStorageEngine.count(new Query(VariantQueryParam.GENE.key(), entry.getKey()));
            System.out.println("queryResult.getDbTime() = " + queryResult.getTime());
            long count = queryResult.first();
            assertEquals(entry.getValue().longValue(), count);
        }

    }

    @Test
    public void queryArchiveTable() {
        final int[] numVariants = {0};
        Map<String, Long> variantCounts = new HashMap<>();
        System.out.println("Query from Archive table");
        dbAdaptor.archiveIterator(studyMetadata.getName(), fileMetadata.getId(), new Query(), new QueryOptions())
                .forEachRemaining(variant -> {
                    System.out.println("Variant from archive = " + variant.toJson());
                    numVariants[0]++;
                    variantCounts.compute(variant.getType().toString(), (s, integer) -> integer == null ? 1 : (integer + 1));
                });
        System.out.println("End query from Archive table");
        fileMetadata.getStats().getTypeCount().forEach((s, l) -> assertEquals(l, variantCounts.getOrDefault(s, 0L)));
        assertEquals(fileMetadata.getStats().getVariantCount().intValue(), numVariants[0]);
    }

    @Test
    public void checkVariantTable() throws IOException {
        System.out.println("Query from HBase : " + dbAdaptor.getVariantTable());
        HBaseManager hm = new HBaseManager(configuration.get());
        int numVariants = hm.act(dbAdaptor.getVariantTable(), table -> {
            int num = 0;
            ResultScanner resultScanner = table.getScanner(GenomeHelper.COLUMN_FAMILY_BYTES);
            for (Result result : resultScanner) {
                Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
                System.out.println("Variant = " + variant);
                num++;
            }
            resultScanner.close();
            return num;
        });
        System.out.println("End query from HBase : " + dbAdaptor.getVariantTable());
        System.out.println(fileMetadata.getStats().getTypeCount());
        long count = TARGET_VARIANT_TYPE_SET.stream()
                .mapToLong(type -> fileMetadata.getStats().getTypeCount().getOrDefault(type.name(), 0L))
                .sum();
//        count  -= 1; // Deletion is in conflict with other variant: 1:10403:ACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAAC:A
        assertEquals(count, numVariants);
    }

    @Test
    public void checkArchiveTable() throws Exception {
        String tableName = getVariantStorageEngine().getArchiveTableName(STUDY_ID);
        System.out.println("Query from archive HBase " + tableName);
        HBaseManager hm = new HBaseManager(configuration.get());
        ArchiveTableHelper archiveHelper = dbAdaptor.getArchiveHelper(studyMetadata.getId(), FILE_ID);
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(archiveHelper.getFileMetadata().toVariantStudyMetadata(STUDY_NAME));
        hm.act(tableName, table -> {
            ResultScanner resultScanner = table.getScanner(GenomeHelper.COLUMN_FAMILY_BYTES);
            for (Result result : resultScanner) {
                System.out.println("VcfSlice = " + Bytes.toString(result.getRow()));
                byte[] value = result.getValue(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getNonRefColumnName());
                if (value != null && value.length > 0) {
                    VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(
                            value);
                    System.out.println(vcfSlice);
                    List<Variant> variants = converter.convert(vcfSlice);
                    for (Variant variant : variants) {
                        System.out.println(variant.toJson());
                    }
                }
                value = result.getValue(GenomeHelper.COLUMN_FAMILY_BYTES, archiveHelper.getRefColumnName());
                if (value != null && value.length > 0) {
                    VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(
                            value);
                    System.out.println(vcfSlice);
                    List<Variant> variants = converter.convert(vcfSlice);
                    for (Variant variant : variants) {
                        System.out.println(variant.toJson());
                    }
                }
            }
            resultScanner.close();
            return null;
        });
        System.out.println("End query from archive HBase " + tableName);
    }

    @Test
    public void checkMeta() throws Exception {
        System.out.println("Get studies");
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        List<String> studyNames = metadataManager.getStudyNames();
        assertEquals(1, studyNames.size());
        for (String studyName : studyNames) {
            System.out.println("studyName = " + studyName);
            StudyMetadata sm = metadataManager.getStudyMetadata(studyName);
            assertEquals(sm.getId(), STUDY_ID);
            assertEquals(sm.getName(), STUDY_NAME);
            assertEquals(Collections.singleton(FILE_ID), metadataManager.getIndexedFiles(STUDY_ID));
            System.out.println("sm = " + sm);
        }
    }

    @Test
    public void printVariants() throws Exception {
        URI outDir = newOutputUri();
        System.out.println("print variants at = " + outDir);
        VariantHbaseTestUtils.printVariants(studyMetadata, dbAdaptor, outDir);
    }

}
