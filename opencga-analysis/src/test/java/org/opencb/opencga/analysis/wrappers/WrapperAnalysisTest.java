package org.opencb.opencga.analysis.wrappers;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.exception.AnalysisException;
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
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

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

    @Test
    public void plinkFisher() throws AnalysisException, IOException {
        Path inDir1 = Paths.get(opencga.createTmpOutdir("_plink1"));

        outDir = Paths.get(opencga.createTmpOutdir("_plink2"));

        InputStream testTped = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("test.tped");
        Files.copy(testTped, inDir1.resolve("test.tped"), StandardCopyOption.REPLACE_EXISTING);
        InputStream testTfam = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("test.tfam");
        Files.copy(testTfam, inDir1.resolve("test.tfam"), StandardCopyOption.REPLACE_EXISTING);

        System.out.println("======> in dir 1 = " + inDir1.toAbsolutePath());
        System.out.println("======> out dir  = " + outDir.toAbsolutePath());

        ObjectMap plinkParams = new ObjectMap();
        plinkParams.put(PlinkWrapperAnalysis.TPED_FILE_PARAM, inDir1.resolve("test.tped"));
        plinkParams.put(PlinkWrapperAnalysis.TFAM_FILE_PARAM, inDir1.resolve("test.tfam"));
        plinkParams.put("fisher", "");
        plinkParams.put("out", "plink-output");

        PlinkWrapperAnalysis plink = new PlinkWrapperAnalysis();
        plink.setUp(opencga.getOpencgaHome().toString(), plinkParams, outDir, clinicalTest.token);

        AnalysisResult result = plink.start();
        System.out.println(result);

        String outputFilename = plinkParams.get("out") + ".assoc.fisher";
        if (!outDir.resolve(outputFilename).toFile().exists()) {
            fail("Output file does not exits: " + outputFilename);
        }
    }

    @Test
    public void plinkFisherCov() throws AnalysisException, IOException {
        Path inDir1 = Paths.get(opencga.createTmpOutdir("_plink1"));
        Path inDir2 = Paths.get(opencga.createTmpOutdir("_plink2"));

        outDir = Paths.get(opencga.createTmpOutdir("_plink3"));

        InputStream testTped = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("test.tped");
        Files.copy(testTped, inDir1.resolve("test.tped"), StandardCopyOption.REPLACE_EXISTING);
        InputStream testTfam = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("test.tfam");
        Files.copy(testTfam, inDir1.resolve("test.tfam"), StandardCopyOption.REPLACE_EXISTING);
        InputStream testCovar = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("test.cov");
        Files.copy(testCovar, inDir2.resolve("test.cov"), StandardCopyOption.REPLACE_EXISTING);

        System.out.println("======> in dir 1 = " + inDir1.toAbsolutePath());
        System.out.println("======> in dir 2 = " + inDir2.toAbsolutePath());
        System.out.println("======> out dir  = " + outDir.toAbsolutePath());

        ObjectMap plinkParams = new ObjectMap();
        plinkParams.put(PlinkWrapperAnalysis.TPED_FILE_PARAM, inDir1.resolve("test.tped"));
        plinkParams.put(PlinkWrapperAnalysis.TFAM_FILE_PARAM, inDir1.resolve("test.tfam"));
        plinkParams.put(PlinkWrapperAnalysis.COVAR_FILE_PARAM, inDir2.resolve("test.cov"));
        plinkParams.put("fisher", "");
        plinkParams.put("covar-number", "3");
        plinkParams.put("out", "plink-output");

        PlinkWrapperAnalysis plink = new PlinkWrapperAnalysis();
        plink.setUp(opencga.getOpencgaHome().toString(), plinkParams, outDir, clinicalTest.token);

        AnalysisResult result = plink.start();
        System.out.println(result);

        String outputFilename = plinkParams.get("out") + ".assoc.fisher";
        if (!outDir.resolve(outputFilename).toFile().exists()) {
            fail("Output file does not exits: " + outputFilename);
        }
    }

    @Test
    public void rvtestsWaldAndScore() throws AnalysisException, IOException {
        Path inDir1 = Paths.get(opencga.createTmpOutdir("_rvtests1"));
        Path inDir2 = Paths.get(opencga.createTmpOutdir("_rvtests2"));

        outDir = Paths.get(opencga.createTmpOutdir("_rvtests3"));

        InputStream vcfIs = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("example.vcf");
        Files.copy(vcfIs, inDir1.resolve("example.vcf"), StandardCopyOption.REPLACE_EXISTING);
        InputStream phenoIs = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("pheno");
        Files.copy(phenoIs, inDir2.resolve("pheno"), StandardCopyOption.REPLACE_EXISTING);

        System.out.println("======> in dir 1 = " + inDir1.toAbsolutePath());
        System.out.println("======> out dir = " + outDir.toAbsolutePath());

        ObjectMap rvtestsParams = new ObjectMap();
        rvtestsParams.put(RvtestsWrapperAnalysis.COMMAND_PARAM, "rvtest");
        rvtestsParams.put(RvtestsWrapperAnalysis.VCF_FILE_PARAM, inDir1.resolve("example.vcf"));
        rvtestsParams.put(RvtestsWrapperAnalysis.PHENOTYPE_FILE_PARAM, inDir2.resolve("pheno"));
        rvtestsParams.put("single", "wald,score");
        rvtestsParams.put("out", "rvtests-output");

        RvtestsWrapperAnalysis rvtests = new RvtestsWrapperAnalysis();
        rvtests.setUp(opencga.getOpencgaHome().toString(), rvtestsParams, outDir, clinicalTest.token);

        AnalysisResult result = rvtests.start();
        System.out.println(result);

//        String outputFilename = rvtestsParams.get("out") + ".assoc.fisher";
//        if (!outDir.resolve(outputFilename).toFile().exists()) {
//            fail("Output file does not exits: " + outputFilename);
//        }
    }

    @Test
    public void rvtestsKinship() throws AnalysisException, IOException {
        Path inDir1 = Paths.get(opencga.createTmpOutdir("_rvtests"));

        outDir = Paths.get(opencga.createTmpOutdir("_rvtests"));

        InputStream vcfIs = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream("example.vcf");
        Files.copy(vcfIs, inDir1.resolve("example.vcf"), StandardCopyOption.REPLACE_EXISTING);

        System.out.println("======> in dir 1 = " + inDir1.toAbsolutePath());
        System.out.println("======> out dir = " + outDir.toAbsolutePath());

        ObjectMap rvtestsParams = new ObjectMap();
        rvtestsParams.put(RvtestsWrapperAnalysis.COMMAND_PARAM, "vcf2kinship");
        rvtestsParams.put(RvtestsWrapperAnalysis.VCF_FILE_PARAM, inDir1.resolve("example.vcf"));
        rvtestsParams.put("bn", "");
        rvtestsParams.put("out", "rvtests-output");

        RvtestsWrapperAnalysis rvtests = new RvtestsWrapperAnalysis();
        rvtests.setUp(opencga.getOpencgaHome().toString(), rvtestsParams, outDir, clinicalTest.token);

        AnalysisResult result = rvtests.start();
        System.out.println(result);

        String outputFilename = rvtestsParams.get("out") + ".kinship";
        if (!outDir.resolve(outputFilename).toFile().exists()) {
            fail("Output file does not exits: " + outputFilename);
        }
    }

    @Test
    public void alignmentPipeline() throws AnalysisException, IOException {
        // bwa index

        // bwa mem

        // samtools sort

        // samtools index

        // deeptools coverate
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