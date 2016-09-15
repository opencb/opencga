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

package org.opencb.opencga.storage.mongodb.alignment;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentFileUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IndexedAlignmentDBAdaptorTest extends GenericTest {


    public static final String DB_NAME = "opencga-alignment-test";
    private static IndexedAlignmentDBAdaptor dbAdaptor;
    //private AlignmentQueryBuilder dbAdaptor;
    private static MongoDBAlignmentStorageManager manager;
    private static Path bamFile;

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void before() throws IOException, FileFormatException, StorageManagerException, IllegalOpenCGACredentialsException {
        StorageConfiguration storageConfiguration = StorageConfiguration
                .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        StorageEtlConfiguration configuration = storageConfiguration.getStorageEngine(MongoDBAlignmentStorageManager.STORAGE_ENGINE_ID).getAlignment();
        manager = new MongoDBAlignmentStorageManager(storageConfiguration);

        MongoCredentials mongoCredentials = new MongoCredentials(configuration.getDatabase(), DB_NAME);
        try (MongoDataStoreManager mongoDataStoreManager = new MongoDataStoreManager(mongoCredentials.getDataStoreServerAddresses())) {
            MongoDataStore mongoDataStore = mongoDataStoreManager.get(DB_NAME);
            mongoDataStoreManager.drop(DB_NAME);
        }

        Path rootDir = temporaryFolder.getRoot().toPath();
        String bamFileName = "HG00096.chrom20.small.bam";
        bamFile = rootDir.resolve(bamFileName);
        System.out.println("bamFile = " + bamFile);
        Files.copy(IndexedAlignmentDBAdaptorTest.class.getClassLoader().getResourceAsStream(bamFileName), bamFile, StandardCopyOption
                .REPLACE_EXISTING);
        AlignmentFileUtils.createIndex(bamFile);

        ObjectMap options = configuration.getOptions();
        options.put(AlignmentStorageManager.Options.FILE_ID.key(), "HG00096");
        options.put(AlignmentStorageManager.Options.DB_NAME.key(), DB_NAME);
        StorageETLResult storageETLResult = manager.index(Collections.singletonList(bamFile.toUri()), rootDir.toUri(), true, true, true).get(0);
        System.out.println("storageETLResult = " + storageETLResult);

//            Path adaptorPath = Paths.get("/media/jacobo/Nusado/opencga/sequence", "human_g1k_v37.fasta.gz.sqlite.db");
        dbAdaptor = (IndexedAlignmentDBAdaptor) manager.getDBAdaptor(DB_NAME);
    }

    @Test
    public void testGetAllAlignmentsByRegion() throws IOException {

        QueryOptions qo = new QueryOptions();
        //qo.put("view_as_pairs", true);

        qo.put(IndexedAlignmentDBAdaptor.QO_BAM_PATH, bamFile.toString());
        qo.put(IndexedAlignmentDBAdaptor.QO_BAI_PATH, bamFile.toString() + ".bai");
        qo.put(IndexedAlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, false);

        //Region region = new Region("20", 20000000, 20000100);
        Region region = new Region("20", 29829000, 29830000);

        QueryResult alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(Arrays.asList(region), qo);
        printQueryResult(alignmentsByRegion);
        jsonQueryResult("HG04239", alignmentsByRegion);

        qo.put(IndexedAlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, true);
        alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(Arrays.asList(new Region("20", 29829000, 29829500)), qo);
        printQueryResult(alignmentsByRegion);
        jsonQueryResult("HG04239", alignmentsByRegion);

        alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(Arrays.asList(new Region("20", 29829500, 29830000)), qo);
        printQueryResult(alignmentsByRegion);
        jsonQueryResult("HG04239", alignmentsByRegion);

    }

    @Test
    public void testGetHistogramCoverageByRegion() throws IOException {
//29337216, 29473005
        QueryOptions qo = new QueryOptions();
        qo.put(IndexedAlignmentDBAdaptor.QO_FILE_ID, "HG00096");
        qo.put(IndexedAlignmentDBAdaptor.QO_INTERVAL_SIZE, 10000);
        qo.put(IndexedAlignmentDBAdaptor.QO_BAM_PATH, bamFile);
        dbAdaptor.getCoverageByRegion(new Region("20", 29829001, 29830000), qo);
        dbAdaptor.getCoverageByRegion(new Region("20", 29830001, 29833000), qo);
        //qo.put(IndexedAlignmentDBAdaptor.QO_HISTOGRAM, false);
        dbAdaptor.getAllIntervalFrequencies(new Region("20", 29800000, 29900000), qo);
        qo.put(IndexedAlignmentDBAdaptor.QO_INCLUDE_COVERAGE, true);
        dbAdaptor.getAllAlignmentsByRegion(Arrays.asList(new Region("20", 29829001, 29830000)), qo);
        dbAdaptor.getAllAlignmentsByRegion(Arrays.asList(new Region("20", 29828951, 29830000)), qo);

    }


    @Test
    public void getAllIntervalFrequenciesAggregateTest() throws IOException {
        QueryOptions qo = new QueryOptions();
        qo.put(IndexedAlignmentDBAdaptor.QO_BAM_PATH, bamFile.toString());
        qo.put(IndexedAlignmentDBAdaptor.QO_FILE_ID, "HG00096");

        jsonQueryResult("aggregate", dbAdaptor.getAllIntervalFrequencies(new Region("20", 50000, 100000), qo));


    }


    @Test
    public void testGetCoverageByRegion() throws IOException {

        QueryOptions qo = new QueryOptions();
        qo.put(IndexedAlignmentDBAdaptor.QO_FILE_ID, "HG00096");
        qo.put(IndexedAlignmentDBAdaptor.QO_BAM_PATH, bamFile.toString());

        //Region region = new Region("20", 20000000, 20000100);

        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 29829000, 29830000), qo));
        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 29829000, 29850000), qo));
        qo.put(IndexedAlignmentDBAdaptor.QO_HISTOGRAM, false);
        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 29829000, 29830000), qo));
        qo.put(IndexedAlignmentDBAdaptor.QO_INTERVAL_SIZE, 1000000);
        printQueryResult(dbAdaptor.getCoverageByRegion(new Region("20", 1, 65000000), qo));

//        System.out.println(coverageByRegion);
//        System.out.println(coverageByRegion.getTime());
    }

    private void jsonQueryResult(String name, QueryResult qr) throws IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(factory);
        jsonObjectMapper.addMixInAnnotations(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        JsonGenerator generator = factory.createGenerator(new FileOutputStream("/tmp/" + name + "." + qr.getId() + ".json"));

        generator.writeObject(qr.getResult());

    }

    private void printQueryResult(QueryResult qr) {
        String s = qr.getResultType();
        System.out.println("qr.getDbTime() = " + qr.getDbTime());
        if (s.equals(MeanCoverage.class.getCanonicalName())) {
            List<MeanCoverage> meanCoverageList = qr.getResult();
            for (MeanCoverage mc : meanCoverageList) {
                System.out.println(mc.getRegion().toString() + " : " + mc.getCoverage());
            }
        } else if (s.equals(RegionCoverage.class.getCanonicalName())) {
            List<RegionCoverage> regionCoverageList = qr.getResult();
            for (RegionCoverage rc : regionCoverageList) {
                System.out.print(new Region(rc.getChromosome(), (int) rc.getStart(), (int) rc.getEnd()).toString() + " (");
                for (int i = 0; i < rc.getAll().length; i++) {
                    System.out.print(rc.getAll()[i] + ",");
                }
                System.out.println(");");

            }
        }
    }

}