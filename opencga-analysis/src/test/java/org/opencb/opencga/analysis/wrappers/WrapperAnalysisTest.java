package org.opencb.opencga.analysis.wrappers;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.storage.core.manager.OpenCGATestExternalResource;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class WrapperAnalysisTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {

    private AbstractClinicalManagerTest clinicalTest;

    Path outDir;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    @Before
    public void setUp() throws Exception {
        clearDB("opencga_test_user_1000G");
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(catalogManagerResource, getVariantStorageEngine());
    }

//    @Test
    public void plinkFisher() throws AnalysisException, IOException {
        outDir = Paths.get(opencga.createTmpOutdir("_plink"));

        InputStream testMap = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("test.map");
        Files.copy(testMap, outDir.resolve("test.map"), StandardCopyOption.REPLACE_EXISTING);
        InputStream testPed = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("test.ped");
        Files.copy(testPed, outDir.resolve("test.ped"), StandardCopyOption.REPLACE_EXISTING);

        System.out.println("======> out dir = " + outDir.toAbsolutePath());

        ObjectMap plinkParams = new ObjectMap();
        plinkParams.put("file", "test");
        plinkParams.put("fisher", "");
        plinkParams.put("out", "plink-output");

        PlinkWrapperAnalysis plink = new PlinkWrapperAnalysis();
        plink.setUp(opencga.getOpencgaHome().toString(), plinkParams, outDir, clinicalTest.token);

        AnalysisResult result = plink.start();
        System.out.println(result);
    }

    @Test
    public void rvtestsWaldAndScore() throws AnalysisException, IOException {
        outDir = Paths.get(opencga.createTmpOutdir("_rvtests"));

        InputStream testMap = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("example.vcf");
        Files.copy(testMap, outDir.resolve("example.vcf"), StandardCopyOption.REPLACE_EXISTING);
        InputStream testPed = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("pheno");
        Files.copy(testPed, outDir.resolve("pheno"), StandardCopyOption.REPLACE_EXISTING);

        System.out.println("======> out dir = " + outDir.toAbsolutePath());

        ObjectMap rvtestsParams = new ObjectMap();
        rvtestsParams.put("inVcf", "example.vcf");
        rvtestsParams.put("pheno", "pheno");
        rvtestsParams.put("single", "wald,score");
        rvtestsParams.put("out", "rvtests-output");

        RvtestsWrapperAnalysis rvtests = new RvtestsWrapperAnalysis();
        rvtests.setUp(opencga.getOpencgaHome().toString(), rvtestsParams, outDir, clinicalTest.token);

        AnalysisResult result = rvtests.start();
        System.out.println(result);
    }

    //    @Test
    public void commandTest() throws IOException {
        outDir = Paths.get(opencga.createTmpOutdir("_docker"));

        System.out.println(System.getenv().entrySet().stream().map(e -> e.getKey() + " : " + e.getValue()).collect(Collectors.joining("\n")));

        String cmdLine = "docker run docker/whalesay cowsay Mi-Mama-Me-Mima";

        System.out.println("out dir = " + outDir.toAbsolutePath());
        Command cmd = new Command(cmdLine)
                .setOutputOutputStream(new DataOutputStream(new FileOutputStream(outDir.toAbsolutePath().resolve("stdout.txt").toFile())))
                .setErrorOutputStream(new DataOutputStream(new FileOutputStream(outDir.toAbsolutePath().resolve("stderr.txt").toFile())));

        cmd.run();
    }
}