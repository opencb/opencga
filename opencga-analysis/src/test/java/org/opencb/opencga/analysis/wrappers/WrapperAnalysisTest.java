package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
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
        rvtestsParams.put(RvtestsWrapperAnalysis.EXECUTABLE_PARAM, "rvtest");
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
        rvtestsParams.put(RvtestsWrapperAnalysis.EXECUTABLE_PARAM, "vcf2kinship");
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
        ObjectMap params;

        Path inDir1 = Paths.get(opencga.createTmpOutdir("_bwa"));

        // bwa index
        System.out.println("-------   bwa index   ------");
        Path outDir1 = Paths.get(opencga.createTmpOutdir("_alignment1"));

        String fastaFilename = "Homo_sapiens.GRCh38.dna.chromosome.MT.fa.gz";
        InputStream refGenomeIs = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream(fastaFilename);
        Files.copy(refGenomeIs, inDir1.resolve(fastaFilename), StandardCopyOption.REPLACE_EXISTING);

        params = new ObjectMap();

        BwaWrapperAnalysis bwa = new BwaWrapperAnalysis();
        bwa.setUp(opencga.getOpencgaHome().toString(), params, outDir1, clinicalTest.token);
        bwa.setCommand("index")
                .setFastaFile(inDir1.resolve(fastaFilename).toString());

        AnalysisResult bwaIndexResult = bwa.start();
        System.out.println(bwaIndexResult);

        assertTrue(Files.exists(outDir1.resolve(fastaFilename + ".bwt")));

        // bwa mem
        System.out.println("-------   bwa mem   ------");
        Path outDir2 = Paths.get(opencga.createTmpOutdir("_alignment2"));

        String fastqFilename = "ERR251000.1K.fastq.gz";
        InputStream fastqIs = WrapperAnalysisTest.class.getClassLoader().getResourceAsStream(fastqFilename);
        Files.copy(fastqIs, inDir1.resolve(fastqFilename), StandardCopyOption.REPLACE_EXISTING);

        params = new ObjectMap();

        bwa = new BwaWrapperAnalysis();
        bwa.setUp(opencga.getOpencgaHome().toString(), params, outDir2, clinicalTest.token);
        bwa.setCommand("mem")
                .setIndexBaseFile(outDir1.resolve(fastaFilename).toString())
                .setFastq1File(inDir1.resolve(fastqFilename).toString())
                .setSamFile(outDir2.resolve("output.sam").toString());

        AnalysisResult bwaMemResult = bwa.start();
        System.out.println(bwaMemResult);

        assertTrue(Files.exists(new File(bwa.getSamFile()).toPath()));

        // samtools view (convert .sam to .bam)
        System.out.println("-------   samtools view   ------");
        Path outDir3 = Paths.get(opencga.createTmpOutdir("_alignment3"));

        params = new ObjectMap();
        params.put("b", "");
        params.put("S", "");

        String bamFile = outDir3.resolve((new File(bwa.getSamFile()).getName()) + ".bam").toString();

        SamtoolsWrapperAnalysis samtools = new SamtoolsWrapperAnalysis();
        samtools.setUp(opencga.getOpencgaHome().toString(), params, outDir3, clinicalTest.token);
        samtools.setCommand("view")
                .setInputFile(bwa.getSamFile())
                .setOutputFile(bamFile);

        AnalysisResult samtoolsViewResult = samtools.start();
        System.out.println(samtoolsViewResult);

        assertTrue(Files.exists(new File(samtools.getOutputFile()).toPath()));

        // samtools sort (.bam)
        System.out.println("-------   samtools sort   ------");
        Path outDir4 = Paths.get(opencga.createTmpOutdir("_alignment4"));

        params = new ObjectMap();

        String sortedBamFile = outDir4.resolve((new File(bwa.getSamFile()).getName()) + ".sorted.bam").toString();

        samtools = new SamtoolsWrapperAnalysis();
        samtools.setUp(opencga.getOpencgaHome().toString(), params, outDir4, clinicalTest.token);
        samtools.setCommand("sort")
                .setInputFile(bamFile)
                .setOutputFile(sortedBamFile);

        AnalysisResult samtoolsSortResult = samtools.start();
        System.out.println(samtoolsSortResult);

        assertTrue(Files.exists(new File(samtools.getOutputFile()).toPath()));

        // samtools index (generate .bai)
        System.out.println("-------   samtools index   ------");
        Path outDir5 = Paths.get(opencga.createTmpOutdir("_alignment5"));

        params = new ObjectMap();

        String baiFile = outDir5.resolve((new File(sortedBamFile).getName()) + ".bai").toString();

        samtools = new SamtoolsWrapperAnalysis();
        samtools.setUp(opencga.getOpencgaHome().toString(), params, outDir5, clinicalTest.token);
        samtools.setCommand("index")
                .setInputFile(sortedBamFile)
                .setOutputFile(baiFile);

        AnalysisResult samtoolsIndexResult = samtools.start();
        System.out.println(samtoolsIndexResult);

        assertTrue(Files.exists(new File(samtools.getOutputFile()).toPath()));

        // deeptools bamCoverage
        System.out.println("-------   deeptools bamCoverage   ------");
        Path outDir6 = Paths.get(opencga.createTmpOutdir("_alignment6"));

        params = new ObjectMap();
        params.put("of", "bigwig");

        FileUtils.copyFile(new java.io.File(baiFile), new java.io.File(outDir4 + "/" + new java.io.File(baiFile).getName()));
        String coverageFile = outDir6.resolve((new File(sortedBamFile).getName()) + ".bw").toString();

        DeeptoolsWrapperAnalysis deeptools = new DeeptoolsWrapperAnalysis();
        deeptools.setUp(opencga.getOpencgaHome().toString(), params, outDir6, clinicalTest.token);
        deeptools.setCommand("bamCoverage")
                .setBamFile(sortedBamFile)
                .setCoverageFile(coverageFile);

        AnalysisResult deepToolsBamCoverageResult = deeptools.start();
        System.out.println(deepToolsBamCoverageResult);

        assertTrue(Files.exists(new File(coverageFile).toPath()));

        // samtools stats (generate stats)
        System.out.println("-------   samtools stats   ------");
        Path outDir7 = Paths.get(opencga.createTmpOutdir("_alignment7"));

        params = new ObjectMap();

        String statsFile = outDir7.resolve((new File(sortedBamFile).getName()) + ".stats").toString();

        samtools = new SamtoolsWrapperAnalysis();
        samtools.setUp(opencga.getOpencgaHome().toString(), params, outDir7, clinicalTest.token);
        samtools.setCommand("stats")
                .setInputFile(sortedBamFile)
                .setOutputFile(statsFile);

        AnalysisResult samtoolsStatsResult = samtools.start();
        System.out.println(samtoolsStatsResult);

        assertTrue(Files.exists(new File(samtools.getOutputFile()).toPath()));
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