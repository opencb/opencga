package org.opencb.opencga.app.cli.analysis;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.analysis.storage.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

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
        projectId = catalogManager.createProject(userId, "p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();

        Map<File.Bioformat, DataStore> datastores = new HashMap<>();
        datastores.put(File.Bioformat.VARIANT, new DataStore(STORAGE_ENGINE, dbNameVariants));
        datastores.put(File.Bioformat.ALIGNMENT, new DataStore(STORAGE_ENGINE, dbNameAlignments));

        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, null, "Study 1", null,
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
        AnalysisMain.privateMain(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", "user@p1:s1:" + file1.getPath()});
        assertEquals(Index.IndexStatus.READY, catalogManager.getFile(file1.getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getStatus());
        job = catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), file1.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());

        // Annotate variants from chr2 (which is not indexed)
        AnalysisMain.privateMain(new String[]{"variant", "annotate", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--filter-chromosome", "2"});

        // Annotate all variants
        AnalysisMain.privateMain(new String[]{"variant", "annotate", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--output-filename", "myAnnot", "-o", String.valueOf(outdirId)});
        File outputFile = catalogManager.getAllFiles(studyId, new Query(CatalogFileDBAdaptor.QueryParams.NAME.key(), "~myAnnot"), null, sessionId).first();
        assertNotNull(outputFile);
        job = catalogManager.getJob(outputFile.getJobId(), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());
        assertEquals(outdirId, job.getOutDirId());

        // Index file2
        AnalysisMain.privateMain(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file2.getId()), "--calculate-stats", "--outdir", String.valueOf(outdirId)});
        assertEquals(Index.IndexStatus.READY, catalogManager.getFile(file2.getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getStatus());
        job = catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), file2.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());
        assertEquals(outdirId, job.getOutDirId());

        // Annotate all variants
        AnalysisMain.privateMain(new String[]{"variant", "annotate", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--outdir-id", String.valueOf(outdirId)});

        // Index file3
        AnalysisMain.privateMain(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file3.getId())});
        assertEquals(Index.IndexStatus.READY, catalogManager.getFile(file3.getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getStatus());
        job = catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), file3.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());
        Assert.assertNotEquals(outdirId, job.getOutDirId());

        // Index file4
        AnalysisMain.privateMain(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file4.getId()), "--calculate-stats", "--queue"});
        assertEquals(Index.IndexStatus.INDEXING, catalogManager.getFile(file4.getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getStatus());
        job = catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), file4.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.PREPARED, job.getStatus().getStatus());
        opencga.runStorageJob(job, sessionId);

        assertEquals(Index.IndexStatus.READY, catalogManager.getFile(file4.getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getStatus());
        job = catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), file4.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());

        // Index file5
        AnalysisMain.privateMain(new String[]{"variant", "index", "--session-id", sessionId, "--file-id", String.valueOf(file5.getId())});
        assertEquals(Index.IndexStatus.READY, catalogManager.getFile(file5.getId(), sessionId).first().getIndex().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getStatus());
        job = catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), file5.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());

        AnalysisMain.privateMain(new String[]{"variant", "stats", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--cohort-ids", "ALL", "--outdir-id", String.valueOf(outdirId)});
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getStatus());

        catalogManager.createCohort(studyId, "coh1", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId);
        catalogManager.createCohort(studyId, "coh2", Cohort.Type.CONTROL_SET, "", file2.getSampleIds(), null, sessionId);

        AnalysisMain.privateMain(new String[]{"variant", "stats", "--session-id", sessionId, "--study-id", String.valueOf(studyId), "--cohort-ids", "coh1", "--outdir-id", String.valueOf(outdirId)});
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "coh1"), null, sessionId).first().getStatus().getStatus());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getAllCohorts(studyId, new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), "coh2"), null, sessionId).first().getStatus().getStatus());

//        AnalysisMain.privateMain(new String[]{"variant", "query", "--session-id", sessionId, "--study", String.valueOf(studyId), "--limit", "10"});
    }

    @Test
    public void testAlignmentIndex() throws CatalogException, IOException {
        Job job;

        File bam = opencga.createFile(studyId, "HG00096.chrom20.small.bam", sessionId);

        // Index file1
        AnalysisMain.privateMain(new String[]{"alignment", "index", "--session-id", sessionId, "--file-id", "user@p1:s1:" + bam.getPath()});
        assertEquals(Index.IndexStatus.READY, catalogManager.getFile(bam.getId(), sessionId).first().getIndex().getStatus().getStatus());
        job = catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), bam.getId()), null, sessionId).first();
        assertEquals(Job.JobStatus.READY, job.getStatus().getStatus());

        AnalysisMain.privateMain(new String[]{"alignment", "query", "--session-id", sessionId, "--file-id", "user@p1:s1:" + bam.getPath(), "--region", "20"});

    }

}