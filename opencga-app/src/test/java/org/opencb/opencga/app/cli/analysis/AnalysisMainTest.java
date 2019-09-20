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

package org.opencb.opencga.app.cli.analysis;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.manager.OpenCGATestExternalResource;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
    private String projectId;
    private String studyId;
    private String outdirId;
    private Logger logger = LoggerFactory.getLogger(AnalysisMainTest.class);
    private Map<File.Bioformat, DataStore> datastores;


    @Before
    public void setUp() throws Exception {
        catalogManager = opencga.getCatalogManager();


        opencga.clearStorageDB(STORAGE_ENGINE, dbNameVariants);
        opencga.clearStorageDB(STORAGE_ENGINE, dbNameAlignments);

        User user = catalogManager.getUserManager().create(userId, "User", "user@email.org", "user", "ACME", null, Account.Type.FULL, null).first();

        sessionId = catalogManager.getUserManager().login(userId, "user");
        projectId = catalogManager.getProjectManager().create("p1", "p1", "Project 1", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).first().getId();

        datastores = new HashMap<>();
        datastores.put(File.Bioformat.VARIANT, new DataStore(STORAGE_ENGINE, dbNameVariants));
        datastores.put(File.Bioformat.ALIGNMENT, new DataStore(STORAGE_ENGINE, dbNameAlignments));


    }

    private void createStudy(Map<File.Bioformat, DataStore> datastores, String studyName) throws CatalogException {
        Study study = catalogManager.getStudyManager().create(projectId, studyName, studyName, studyName, Study.Type.CASE_CONTROL, null,
                "Study " +
                        "1", null, null, null, null, datastores, null, Collections.singletonMap(VariantStorageEngine.Options.AGGREGATED_TYPE.key(),
                        Aggregation.NONE), null, sessionId).first();
        studyId = study.getId();
        outdirId = catalogManager.getFileManager().createFolder(studyId, Paths.get("data", "index").toString(), null,
                true, null, QueryOptions.empty(), sessionId).first().getId();

    }

    @Test
    public void testVariantIndex() throws Exception {
//        Job job;
        createStudy(datastores, "s1");
        File file1 = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file2 = opencga.createFile(studyId, "1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file3 = opencga.createFile(studyId, "1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file4 = opencga.createFile(studyId, "1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
        File file5 = opencga.createFile(studyId, "1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        // Index file1
        execute("variant", "index",
                "--session-id", sessionId,
                "--study", "user@p1:s1",
                "-o", opencga.createTmpOutdir(studyId, "index_1", sessionId),
                "--file", file1.getPath());
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFileManager().get(studyId, file1.getId(), null, sessionId).first().getIndex().getStatus().getName());

        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "ALL"), null, sessionId).first().getStatus().getName());

//        job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file1.getId()), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        // Annotate variants from chr2 (which is not indexed)
        execute("variant", "annotate",
                "--session-id", sessionId,
                "--study", studyId,
                "-o", opencga.createTmpOutdir(studyId, "annot_2", sessionId),
                "--filter-region", "2");

        // Annotate all variants
        execute("variant", "annotate",
                "--session-id", sessionId,
                "--study", studyId,
                "-o", opencga.createTmpOutdir(studyId, "annot_all", sessionId),
                "--output-filename", "myAnnot",
                "--path", outdirId);

        File outputFile = catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.NAME.key(),
                "~myAnnot"), null, sessionId).first();
        assertNotNull(outputFile);
//        job = catalogManager.getJobManager().get(outputFile.getJob().getId(), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
//        assertEquals(outdirId, job.getOutDir().getId());

        // Index file2
        execute("variant", "index",
                "--session-id", sessionId,
                "--file", file2.getId(),
                "--calculate-stats",
                "-o", opencga.createTmpOutdir(studyId, "index_2", sessionId));

        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFileManager().get(studyId, file2.getId(), null, sessionId).first().getIndex()
                .getStatus().getName());
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "ALL"), null, sessionId).first().getStatus().getName());

//        job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file2.getId()), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
//        assertEquals(outdirId, job.getOutDir().getId());

        // Annotate all variants
        execute("variant", "annotate",
                "--session-id", sessionId,
                "--study", studyId,
                "-o", opencga.createTmpOutdir(studyId, "annot_all_2", sessionId));

        // Index file3
        execute("variant", "index",
                "--session-id", sessionId,
                "--file", String.valueOf(file3.getId()),
                "-o", opencga.createTmpOutdir(studyId, "index_3", sessionId));
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFileManager().get(studyId, file3.getId(), null, sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "ALL"), null, sessionId).first().getStatus().getName());
//        job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file3.getId()), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());
//        assertNotEquals(outdirId, job.getOutDir().getId());

//        // Index file4 and stats
//        execute("variant", "index",
//                "--session-id", sessionId,
//                "--file", String.valueOf(file4.getId()),
//                "--calculate-stats", "--queue");
//        assertEquals(FileIndex.IndexStatus.INDEXING, catalogManager.getFileManager().get(file4.getId(), null, sessionId).first().getIndex().getStatus().getName());
//        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
//        Job job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file4.getId()), null, sessionId).first();
//        assertEquals(Job.JobStatus.PREPARED, job.getStatus().getName());
//        opencga.runStorageJob(job, sessionId);
//
//        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFileManager().get(file4.getId(), null, sessionId).first().getIndex().getStatus().getName());
//        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), "ALL"), null, sessionId).first().getStatus().getName());
//        job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file4.getId()), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        // Index file5 and annotation
        execute("variant", "index",
                "--session-id", sessionId,
                "--file", file5.getId(),
                "-o", opencga.createTmpOutdir(studyId, "index_5", sessionId),
                "--annotate");
        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFileManager().get(studyId, file5.getId(), null, sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.INVALID, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "ALL"), null, sessionId).first().getStatus().getName());

//        job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file5.getId()), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        execute("variant", "stats",
                "--session-id", sessionId,
                "--study", studyId,
                "--cohort-ids", "ALL",
                "-o", opencga.createTmpOutdir(studyId, "stats_all", sessionId));
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "ALL"), null, sessionId).first().getStatus().getName());


        catalogManager.getCohortManager().create(studyId, "coh1", Study.Type.CONTROL_SET, "", file1.getSamples(), null, null,
                sessionId);
        catalogManager.getCohortManager().create(studyId, "coh2", Study.Type.CONTROL_SET, "", file2.getSamples(), null, null,
                sessionId);

        execute("variant", "stats",
                "--session-id", sessionId,
                "--study", studyId,
                "--cohort-ids", "coh1",
                "-o", opencga.createTmpOutdir(studyId, "stats_coh1", sessionId));
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "coh1"), null, sessionId).first().getStatus().getName());
        assertEquals(Cohort.CohortStatus.NONE, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "coh2"), null, sessionId).first().getStatus().getName());

//        execute(new String[]{"variant", "query", "--session-id", sessionId, "--study", studyId, "--limit", "10"});
    }

    @Test
    public void testVariantIndexAndQuery() throws CatalogException, IOException {
        createStudy(datastores, "s2");
        File file1 = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
//        File file1 = opencga.createFile(studyId, "variant-test-file.vcf.gz", sessionId);
//        File file1 = opencga.createFile(studyId, "100k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);
//        File file1 = opencga.createFile(studyId, "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        QueryResult<Sample> allSamples = catalogManager.getSampleManager().get(studyId, new Query(), new QueryOptions(),
                sessionId);
        String c1 = catalogManager.getCohortManager().create(studyId, "C1", Study.Type.CONTROL_SET, "", allSamples.getResult().subList(0,
                allSamples.getResult().size() / 2), null, null, sessionId).first().getId();
        String c2 = catalogManager.getCohortManager().create(studyId, "C2", Study.Type.CONTROL_SET, "", allSamples.getResult().subList(allSamples.getResult().size()
                / 2 + 1, allSamples.getResult().size()), null, null, sessionId).first().getId();
        String c3 = catalogManager.getCohortManager().create(studyId, "C3", Study.Type.CONTROL_SET, "", allSamples.getResult().subList(0, 1), null,
                null, sessionId).first().getId();
        Sample sample = catalogManager.getSampleManager().create(studyId, new Sample().setId("Sample"), null, sessionId).first();
        String c4 = catalogManager.getCohortManager().create(studyId, "C4", Study.Type.CONTROL_SET, "", Collections.singletonList(sample),
                null, null, sessionId).first().getId();

        // Index file1
        execute("variant", "index",
                "--session-id", sessionId,
                "--study", studyId,
                "--file", file1.getName(),
                "-o", opencga.createTmpOutdir(studyId, "index", sessionId),
                "--calculate-stats",
                "--annotate");

        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFileManager().get(studyId, file1.getId(), null, sessionId).first().getIndex().getStatus().getName());
        assertEquals(Cohort.CohortStatus.READY, catalogManager.getCohortManager().get(studyId,
                new Query(CohortDBAdaptor.QueryParams.ID.key(), "ALL"), null, sessionId).first().getStatus().getName());

//        job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), file1.getId()), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        execute("variant", "stats",
                "--session-id", sessionId,
                "--study", studyId,
                "--cohort-ids", c1 + "," + c2 + "," + c3 + "," + c4,
                "-o", opencga.createTmpOutdir(studyId, "stats", sessionId));

//        execute(new String[]{"variant", "query", "--session-id", sessionId, "--output-sample", "35,36", "--limit", "10"});


        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: cellbase");
        System.out.println("------------------------------------------------------");
        execute("variant", "export-frequencies", "--session-id", sessionId, "--limit", "1000", "--output-format", "cellbase");

//        System.out.println("------------------------------------------------------");
//        System.out.println("Export output format: avro");
//        System.out.println("------------------------------------------------------");
//        execute(new String[]{"variant", "query", "--session-id", sessionId, "--output-format", "avro", "--output", "/tmp/100k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz.avro.snappy"});

        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: avro.snappy");
        System.out.println("------------------------------------------------------");
        execute("variant", "query", "--session-id", sessionId, "--output-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "avro.snappy");

        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: vcf");
        System.out.println("------------------------------------------------------");
        execute("variant", "query", "--session-id", sessionId, "--output-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "vcf");

        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: cellbase (populationFrequencies)");
        System.out.println("------------------------------------------------------");
        execute("variant", "query", "--session-id", sessionId, "--output-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "cellbase");

        System.out.println("------------------------------------------------------");
        System.out.println("Export output format: tsv");
        System.out.println("------------------------------------------------------");
        execute("variant", "query", "--session-id", sessionId, "--output-sample", "HG00096,HG00097", "--limit", "10", "--output-format", "stats");

        System.out.println("------------------------------------------------------");
        System.out.println("Export-frequencies output format: tsv");
        System.out.println("------------------------------------------------------");
        execute("variant", "export-frequencies", "--session-id", sessionId, "--limit", "10", "--output-format", "tsv");

        System.out.println("------------------------------------------------------");
        System.out.println("Export-frequencies output format: vcf");
        System.out.println("------------------------------------------------------");
        execute("variant", "export-frequencies", "--session-id", sessionId, "--limit", "10", "--output-format", "vcf");

        System.out.println("------------------------------------------------------");
        System.out.println("Export-frequencies output format: default");
        System.out.println("------------------------------------------------------");
        execute("variant", "export-frequencies", "--session-id", sessionId, "--limit", "10");

    }

    @Test
    public void testAlignmentIndex() throws CatalogException, IOException {
//        Job job;

        File bam = opencga.createFile(studyId, "HG00096.chrom20.small.bam", sessionId);
//        File bai = opencga.createFile(studyId, "HG00096.chrom20.small.bam.bai", sessionId);

        String temporalDir = opencga.createTmpOutdir(studyId, "index", sessionId);

        // Index file1
        execute("alignment", "index",
                "--session-id", sessionId,
                "--study", studyId,
                "--file", bam.getName(),
                "-o", temporalDir);


//        assertEquals(FileIndex.IndexStatus.READY, catalogManager.getFileManager().get(studyId, bam.getId(), null, sessionId).first().getIndex().getStatus().getName());
//        job = catalogManager.getJobManager().get(studyId, new Query(JobDBAdaptor.QueryParams.INPUT.key(), bam.getUid()), null, sessionId).first();
//        assertEquals(Job.JobStatus.READY, job.getStatus().getName());

        assertEquals(3, Files.list(Paths.get(temporalDir)).collect(Collectors.toList()).size());
        assertTrue(Files.exists(Paths.get(temporalDir).resolve("HG00096.chrom20.small.bam.bai")));
        assertTrue(Files.exists(Paths.get(temporalDir).resolve("HG00096.chrom20.small.bam.bw")));
        assertTrue(Files.exists(Paths.get(temporalDir).resolve("status.json")));

//        execute("alignment", "query", "--session-id", sessionId, "--file", "user@p1:s1:" + bam.getPath(), "--region", "20");

    }


    public int execute(String... args) {
        int exitValue = AnalysisMain.privateMain(args);
        assertEquals(0, exitValue);
        return exitValue;
    }

}