/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.*;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@Ignore
public abstract class VariantDBAdaptorTest extends VariantStorageManagerTestUtils {

    public static final String VCF_TEST_FILE_NAME = "variant-test-file.vcf.gz";
    public static final int NUM_VARIANTS = 999;
    protected static URI inputUri;
    protected static ETLResult etlResult = null;
    protected VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected static StudyConfiguration studyConfiguration;

    @BeforeClass
    public static void beforeClass() throws IOException {
        etlResult = null;
        Path rootDir = getTmpRootDir();
        Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAME);
        Files.copy(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME), inputPath, StandardCopyOption.REPLACE_EXISTING);
        inputUri = inputPath.toUri();
    }

    @Override
    @Before
    public void before() throws Exception {
        if (etlResult == null) {
            studyConfiguration = newStudyConfiguration();
//            variantSource = new VariantSource(inputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
            clearDB(DB_NAME);
            etlResult = runDefaultETL(inputUri, getVariantStorageManager(), studyConfiguration);
        }
        dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
    }

    @After
    public void after() {
        dbAdaptor.close();
    }

    @Test
    public void testGetAllVariants() {
        options = new QueryOptions("limit", 1);
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());
        assertEquals(1, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariants_id() {
        // This test queries a single ID with no more options
        queryResult = dbAdaptor.getVariantById("rs1137005", null);
        Variant variant = queryResult.first();
        assertEquals(1, queryResult.getNumResults());
        assertEquals(variant.getStart(), 1650807);
        assertTrue(variant.getIds().contains("rs1137005"));

        options = new QueryOptions(VariantDBAdaptor.ID, "rs1137005,rs150535390");
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(2, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariants_region() {
        options = new QueryOptions(VariantDBAdaptor.REGION, "1:13910417-13910417,1:165389129-165389129");
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(2, queryResult.getNumResults());

        options = new QueryOptions(VariantDBAdaptor.REGION, Arrays.asList("1:13910417-13910417", "1:165389129-165389129"));
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(2, queryResult.getNumResults());

        options = new QueryOptions(VariantDBAdaptor.REGION,
                Arrays.asList(Region.parseRegion("1:13910417-13910417"), Region.parseRegion("1:165389129-165389129")));
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(2, queryResult.getNumResults());

        options = new QueryOptions(VariantDBAdaptor.REGION, "1:14000000-160000000");
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(64, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariants_files() {
        options = new QueryOptions(VariantDBAdaptor.FILES, 6);
        options.put("limit", 1);
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        options = new QueryOptions(VariantDBAdaptor.FILES, -1);
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals("There is no file with ID -1", 0, queryResult.getNumResults());
    }

    @Test
    public void testIterator() {
        int numVariants = 0;
        for (Variant variant : dbAdaptor) {
            numVariants++;
            VariantSourceEntry entry = variant.getSourceEntries().entrySet().iterator().next().getValue();
            assertEquals("6", entry.getFileId());
            assertEquals("5", entry.getStudyId());
            assertEquals(studyConfiguration.getSampleIds().keySet(), entry.getSampleNames());
        }
        assertEquals(NUM_VARIANTS, numVariants);
    }

    @Test
    public void testGetAllVariants_genotypes() {
        Integer na19600 = studyConfiguration.getSampleIds().get("NA19600");
        Integer na19685 = studyConfiguration.getSampleIds().get("NA19685");

        options = new QueryOptions(VariantDBAdaptor.GENOTYPE, na19600+":1|1");
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(282, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getSourceEntries().forEach((s, vse) -> assertEquals("1|1", vse.getSampleData("NA19600", "GT"))));


        //get for each genotype. Should return all variants
        options = new QueryOptions(VariantDBAdaptor.GENOTYPE, na19600+":0|0,0|1,1|0,1|1,./.");
        options.add("limit", 1);
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        //Get all missing genotypes for sample na19600
        options = new QueryOptions(VariantDBAdaptor.GENOTYPE, na19600+":./.");
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(9, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getSourceEntries().forEach((s, vse) -> assertEquals("./.", vse.getSampleData("NA19600", "GT"))));

        //This works, but is incorrect. Better use "./."
        options = new QueryOptions(VariantDBAdaptor.GENOTYPE, na19600+":-1/-1");
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(9, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getSourceEntries().forEach((s, vse) -> assertEquals("./.", vse.getSampleData("NA19600", "GT"))));


        //Get all variants with 1|1 for na19600 and 0|0 or 1|0 for na19685
        options = new QueryOptions(VariantDBAdaptor.GENOTYPE, na19600+":1|1"+";"+na19685+":0|0,1|0");
        queryResult = dbAdaptor.getAllVariants(options);
        assertEquals(14, queryResult.getNumTotalResults());
        queryResult.getResult().forEach(v -> v.getSourceEntries().forEach((s, vse) -> {
            assertEquals("1|1", vse.getSampleData("NA19600", "GT"));
            assertTrue(Arrays.asList("0|0", "1|0").contains(vse.getSampleData("NA19685", "GT")));
        }));
    }

/*
    @Test
    public void testGetAllVariants() {
        QueryOptions options = new QueryOptions();
        options.put("id", "rs1137005,rs150535390");
        options.put("region", "1:13910417-13910417,1:165389129-165389129");
        options.put("gene", "RCC2,HRNR");
        options.put("mgf", "<=0.5");

        QueryResult queryResult = vqb.getAllVariants(options);
        assertEquals(5, queryResult.getNumResults());
//        System.out.println(queryResult);
    }

    @Test
    public void testGetVariantById() {
        QueryResult queryResult;

        // This test queries a single ID with no more options
        queryResult = vqb.getVariantById("rs1137005", null);
        Variant object = (Variant) queryResult.getResult().get(0);
        assertEquals(object.getStart(), 1650807);

        // This test adds a few other options. Options related with genomic coordinates must be
        // added as a logical OR while others as and logical AND.
        QueryOptions options = new QueryOptions("type", "SNV");
        options.put("id", "rs150535390");
        options.put("region", "1:13910417-13910417,1:165389129-165389129");
        options.put("gene", "RCC2,HRNR");
        options.put("mgf", "<=0.5");

        queryResult = vqb.getVariantById("rs1137005", options);
        assertEquals(5, queryResult.getNumResults());
//        System.out.println("queryResult = " + queryResult);
    }

    @Test
    public void testGetAllVariantsByRegion() {
        QueryResult queryResult;

        // Basic queries
        queryResult = vqb.getAllVariantsByRegion(new Region("1:1000000-2000000"), null);
        assertEquals(3, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("1:10000000-20000000"), null);
        assertEquals(11, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("3:1-200000000"), null);
        assertEquals(50, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("X:1-200000000"), null);
        assertEquals(11, queryResult.getNumResults());

        // Exactly in the limits
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238441-7980390"), null);
        assertEquals(5, queryResult.getNumResults());

        // Just inside the limits
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238440-7980391"), null);
        assertEquals(5, queryResult.getNumResults());

        // Just outside the limits
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238441-7980389"), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238442-7980390"), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegion(new Region("20:238442-7980389"), null);
        assertEquals(3, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariantFrequencyByRegion() {
        QueryResult queryResult;

        QueryOptions options = new QueryOptions("interval", 100000);
        options.put("mgf", "<=0.5");
        // Basic queries
        queryResult = vqb.getVariantFrequencyByRegion(new Region("1:10000000-20000000"), options);
        System.out.println("queryResult = " + queryResult);
//        assertEquals(3, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariantsByRegionAndStudy() {
        QueryResult queryResult;

        // Basic queries
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("1:1000000-2000000"), Arrays.asList(study.getStudyId()), null);
        System.out.println("queryResult = " + queryResult);
        assertEquals(3, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("1:10000000-20000000"), Arrays.asList(study.getStudyId()), null);
        assertEquals(11, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("3:1-200000000"), Arrays.asList(study.getStudyId()), null);
        assertEquals(50, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("X:1-200000000"), Arrays.asList(study.getStudyId()), null);
        assertEquals(11, queryResult.getNumResults());

        // Exactly in the limits
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238441-7980390"), Arrays.asList(study.getStudyId()), null);
        assertEquals(5, queryResult.getNumResults());

        // Just inside the limits
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238440-7980391"), Arrays.asList(study.getStudyId()), null);
        assertEquals(5, queryResult.getNumResults());

        // Just outside the limits
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238441-7980389"), Arrays.asList(study.getStudyId()), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238442-7980390"), Arrays.asList(study.getStudyId()), null);
        assertEquals(4, queryResult.getNumResults());
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("20:238442-7980389"), Arrays.asList(study.getStudyId()), null);
        assertEquals(3, queryResult.getNumResults());

        // Non-existing study
        queryResult = vqb.getAllVariantsByRegionAndStudies(new Region("1:1000000-2000000"), Arrays.asList("FalseStudy"), null);
        assertEquals(0, queryResult.getNumResults());
    }

    @Test
    public void testGetAllVariantsByGene() {
        QueryResult queryResult;

        // Gene present in the dataset
        queryResult = vqb.getAllVariantsByGene("MIB2", null);
        assertNotEquals(0, queryResult.getNumResults());
        List<Variant> variantsInGene = queryResult.getResult();

        for (Variant v : variantsInGene) {
            assertEquals("1", v.getChromosome());
        }

        // Gene not present in the dataset
        queryResult = vqb.getAllVariantsByGene("NonExistingGene", null);
        assertEquals(0, queryResult.getNumResults());
    }

    @Test
    public void testGetMostAffectedGenes() {
//        QueryResult queryResult = vqb.getMostAffectedGenes(10, null);

        QueryResult queryResult = vqb.groupBy("gene", null);
        assertEquals(10, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        System.out.println("queryResult = " + queryResult);

        List<DBObject> result = queryResult.getResult();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) >= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetLeastAffectedGenes() {
        QueryResult queryResult = vqb.getLeastAffectedGenes(10, null);
        assertEquals(10, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        List<DBObject> result = queryResult.getResult();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) <= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetTopConsequenceTypes() {
        QueryResult queryResult = vqb.getTopConsequenceTypes(5, null);
        assertEquals(5, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        List<DBObject> result = queryResult.getResult();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) >= ((int) object.get("count")));
        }
    }

    @Test
    public void testGetBottomConsequenceTypes() {
        QueryResult queryResult = vqb.getBottomConsequenceTypes(5, null);
        assertEquals(5, queryResult.getNumResults());
        System.out.println(Arrays.deepToString(queryResult.getResult().toArray()));

        List<DBObject> result = queryResult.getResult();
        for (int i = 1; i < queryResult.getNumResults(); i++) {
            DBObject prevObject = result.get(i-1);
            DBObject object = result.get(i);
            assertTrue(((int) prevObject.get("count")) <= ((int) object.get("count")));
        }
    }



//    @Test
//    public void testGetRecords() throws Exception {
//
//        Map<String, String> opts = new HashMap<>();
//        opts.put("studyId", "aaleman_-_XOidGTJMUq1Cr1J");
////        opts.put("region_list", "6:1-15021068");
////        opts.put("sampleGT_D801[]", "1/1,0/1");
////        opts.put("sampleGT_muestra_B[]", "0/1");
////        opts.put("conseq_type[]", "non_synonymous_codon,intron_variant");
////        opts.put("mend_error", "1");
////        opts.put("option_mend_error", ">=");
////        opts.put("maf", "0.1");
////        opts.put("option_maf", "<=");
//
//        MutableInt count = new MutableInt(-1);
//
//        QueryResult<VariantInfo> records = ((VariantMongoDBAdaptor) vqb).getRecordsMongo(1, 0, 25, count, opts);
////
//        System.out.println(records.getResult().get(0).getSampleGenotypes());
//    }
//
//    @Test
//    public void testAnalysisInfo() throws Exception {
//
//        QueryResult<VariantAnalysisInfo> res = ((VariantMongoDBAdaptor) vqb).getAnalysisInfo("aaleman_-_XOidGTJMUq1Cr1J");
//        VariantAnalysisInfo vi = res.getResult().get(0);
//
//        System.out.println("vi.getSamples() = " + vi.getSamples());
//        System.out.println("vi.getConsequenceTypes() = " + vi.getConsequenceTypes());
//        System.out.println("vi.getGlobalStats() = " + vi.getGlobalStats());
//
//
//    }
*/


}
