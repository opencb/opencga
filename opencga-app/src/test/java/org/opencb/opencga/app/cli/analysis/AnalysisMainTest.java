package org.opencb.opencga.app.cli.analysis;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.storage.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created on 09/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnalysisMainTest {


    public static final String STORAGE_ENGINE = "mongodb";
    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();


    private CatalogManager catalogManager;
    private final String userId = "user";
    private final String dbNameVariants = "opencga_variants_test";
    private final String dbNameAlignments = "opencga_alignments_test";
    private String sessionId;
    private long projectId;
    private long studyId;
    private long outdirId;
    private Logger logger = LoggerFactory.getLogger(AnalysisMainTest.class);


    @Before
    public void setUp() throws Exception {
        catalogManager = opencga.getCatalogManager();


        opencga.clearStorageDB(STORAGE_ENGINE, dbNameVariants);
        opencga.clearStorageDB(STORAGE_ENGINE, dbNameAlignments);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject("p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();

        Map<File.Bioformat, DataStore> datastores = new HashMap<>();
        datastores.put(File.Bioformat.VARIANT, new DataStore(STORAGE_ENGINE, dbNameVariants));
        datastores.put(File.Bioformat.ALIGNMENT, new DataStore(STORAGE_ENGINE, dbNameAlignments));

        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, "Study 1", null,
                null, null, null, datastores, null,
                Collections.singletonMap(VariantStorageManager.Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.NONE),
                null, sessionId).first().getId();
        outdirId = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first().getId();
    }

    @Test
    public void testVariantIndex() throws Exception {
        Job job;

        File file1 = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file2 = opencga.createFile(studyId, "1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file3 = opencga.createFile(studyId, "1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file4 = opencga.createFile(studyId, "1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file5 = opencga.createFile(studyId, "1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        // Index file1
        execute(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", "user@p1:s1:" + file1.getPath()});
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(file1.getId(), sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file1.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        // Annotate variants from chr2 (which is not indexed)
        execute(new String[]{"variant", "annotate", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--filter-chromosome", "2"});

        // Annotate all variants
        execute(new String[]{"variant", "annotate", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--output-filename", "myAnnot", "-o", String.valueOf(outdirId)});
        File outputFile = catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.NAME.key(), "~myAnnot"), null, sessionId).first();
        assertNotNull(outputFile);
        job = catalogManager.getJob(outputFile.getJobId(), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
        assertEquals(outdirId, job.getOutDirId());

        // Index file2
        execute(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file2.getId()), "--calculate-stats", "--outdir", String.valueOf(outdirId)});
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(file2.getId(), sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file2.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
        assertEquals(outdirId, job.getOutDirId());

        // Annotate all variants
        execute(new String[]{"variant", "annotate", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--outdir-id", String.valueOf(outdirId)});

        // Index file3
        execute(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file3.getId())});
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(file3.getId(), sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file3.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
        Assert.assertNotEquals(outdirId, job.getOutDirId());

        // Index file4 and stats
        execute(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file4.getId()), "--calculate-stats", "--queue"});
        assertEquals(FileIndex.IndexStatus.INDEXING, catalogManager.getFile(file4.getId(), sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file4.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.PREPARED, job.getStatus().getName());
        opencga.runStorageJob(job, sessionId);

        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(file4.getId(), sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file4.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        // Index file5 and annotation
        execute(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file5.getId()), "--annotate"});
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(file5.getId(), sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file5.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        execute(new String[]{"variant", "stats", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--cohort-ids", "ALL", "--outdir-id", String.valueOf(outdirId)});
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());

        catalogManager.createCohort(studyId, "coh1", Study.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId);
        catalogManager.createCohort(studyId, "coh2", Study.Type.CONTROL_SET, "", file2.getSampleIds(), null, sessionId);

        execute(new String[]{"variant", "stats", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--cohort-ids", "coh1", "--outdir-id", String.valueOf(outdirId)});
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "coh1"), null, sessionId).first().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "coh2"), null, sessionId).first().getStatus().getName());

//        execute(new String[]{"variant", "query", "--session-id", sessionId, "--study", String.valueOf(studyId), "--limit", "10"});
    }

    @Test
    public void testVariantIndexAndQuery() throws CatalogException, IOException {
        Job job;

        File file1 = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
//        File file1 = opencga.createFile(studyId, "variant-test-file.vcf.gz", sessionId);
//        File file1 = opencga.createFile(studyId, "100k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
//        File file1 = opencga.createFile(studyId, "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        QueryResult<Sample> allSamples = catalogManager.getAllSamples(studyId, new Query(), new QueryOptions(), sessionId);
        List<Long> sampleIds = allSamples.getResult().stream().map(Sample::getId).collect(Collectors.toList());
        Long c1 = catalogManager.createCohort(studyId, "C1", Study.Type.CONTROL_SET, "", sampleIds.subList(0, sampleIds.size() / 2), null, sessionId).first().getId();
        Long c2 = catalogManager.createCohort(studyId, "C2", Study.Type.CONTROL_SET, "", sampleIds.subList(sampleIds.size() / 2 + 1, sampleIds.size()), null, sessionId).first().getId();
        Long c3 = catalogManager.createCohort(studyId, "C3", Study.Type.CONTROL_SET, "", sampleIds.subList(0, 1), null, sessionId).first().getId();
        Sample sample = catalogManager.createSample(studyId, "Sample", "", "", null, null, sessionId).first();
        Long c4 = catalogManager.createCohort(studyId, "C4", Study.Type.CONTROL_SET, "", Collections.singletonList(sample.getId()), null, sessionId).first().getId();

        // Index file1
        execute(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", "user@p1:s1:" + file1.getPath(), "--calculate-stats", "--annotate"});
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(file1.getId(), sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file1.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        execute(new String[]{"variant", "stats", "--session-id", sessionId, "--study-id", "user@p1:s1", "--cohort-ids", c1 + "," + c2 + "," + c3 + "," + c4});

//        execute(new String[]{"variant", "query", "--session-id", sessionId, "--return-sample", "35,36", "--limit", "10"});


        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: cellbase");
        System.out.println("------------------------------------------------------");
        execute(new String[]{"variant", "export-frequencies", "--session-id", sessionId, "--limit", "1000", "--output-format", "cellbase"});
//        System.out.println("------------------------------------------------------");
//        System.out.println("Export output format: avro");
//        System.out.println("------------------------------------------------------");
//        execute(new String[]{"variant", "query", "--session-id", sessionId, "--output-format", "avro", "--output", "/tmp/100k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz.avro.snappy"});
        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: avro.snappy");
        System.out.println("------------------------------------------------------");
        execute(new String[]{"variant", "query", "--session-id", sessionId, "--return-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "avro.snappy"});
        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: vcf");
        System.out.println("------------------------------------------------------");
        execute(new String[]{"variant", "query", "--session-id", sessionId, "--return-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "vcf"});
        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: cellbase (populationFrequencies)");
        System.out.println("------------------------------------------------------");
        execute(new String[]{"variant", "query", "--session-id", sessionId, "--return-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "cellbase"});
        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: tsv");
        System.out.println("------------------------------------------------------");
        execute(new String[]{"variant", "query", "--session-id", sessionId, "--return-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "stats"});
        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: tsv");
        System.out.println("------------------------------------------------------");
        execute(new String[]{"variant", "export-frequencies", "--session-id", sessionId, "--limit", "10"});
        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: vcf");
        System.out.println("------------------------------------------------------");
        execute(new String[]{"variant", "export-frequencies", "--session-id", sessionId, "--limit", "10", "--output-format", "vcf"});

    }

    @Test
    public void testAlignmentIndex() throws CatalogException, IOException {
        Job job;

        File bam = opencga.createFile(studyId, "HG00096.chrom20.small.bam", sessionId);

        // Index file1
        execute(new String[]{"alignment", "index", "--session-id", sessionId, "--file-id", "user@p1:s1:" + bam.getPath()});
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFile(bam.getId(), sessionId).first().getIndex().getStatus().getName());
        job = catalogManager.getAllJobs(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), bam.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        execute(new String[]{"alignment", "query", "--session-id", sessionId, "--file-id", "user@p1:s1:" + bam.getPath(), "--region", "20"});

    }

    public int execute(String[] args) {
        int exitValue = AnalysisMain.privateMain(args);
        assertEquals(0, exitValue);
        return exitValue;
    }

}