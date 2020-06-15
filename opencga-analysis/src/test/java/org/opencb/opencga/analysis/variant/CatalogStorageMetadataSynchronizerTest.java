/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant;

import org.junit.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileIndex;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created by hpccoll1 on 16/07/15.
 */
public class CatalogStorageMetadataSynchronizerTest {
    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    static private CatalogManager catalogManager;
    static private String sessionId;
    static private String projectId;
    static private String studyId;
    static private long studyUid;
    static private FileMetadataReader fileMetadataReader;
    static private FileUtils catalogFileUtils;
    static private long outputId;
    static Logger logger = LoggerFactory.getLogger(CatalogStorageMetadataSynchronizerTest.class);
    static private String catalogPropertiesFile;
    static private final String userId = "user";
    static private List<File> files = new ArrayList<>();
    static private LinkedHashSet<String> indexedFiles = new LinkedHashSet<>();
    private static String cohortId;
    private static long cohortUid;
    private VariantStorageMetadataManager metadataManager;
    private CatalogStorageMetadataSynchronizer studyConfigurationFactory;

    @BeforeClass
    public static void beforeClass() throws Exception {
//        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
//        stderr.setThreshold(Level.toLevel("debug"));

        catalogManager = catalogManagerExternalResource.getCatalogManager();
        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new FileUtils(catalogManager);

        User user = catalogManager.getUserManager().create(userId, "User", "user@email.org", "user", "ACME", null, Account.AccountType.FULL, null).first();

        sessionId = catalogManager.getUserManager().login(userId, "user").getToken();
        projectId = catalogManager.getProjectManager().create("p1", "p1", "Project 1", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), sessionId).first().getId();
        Study study = catalogManager.getStudyManager().create(projectId, "s1", null, "s1", "Study " + "1", null, null,
                null, null, null, sessionId).first();
        studyId = study.getFqn();
        studyUid = study.getUid();
        outputId = catalogManager.getFileManager().createFolder(studyId, Paths.get("data", "index").toString(),
                true, null, QueryOptions.empty(), sessionId).first().getUid();
//        files.add(create("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
//        files.add(create("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
//        files.add(create("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
//        files.add(create("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
//        files.add(create("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        String cohortName = "ALL";
        Cohort cohort = catalogManager.getCohortManager().create(studyId, cohortName, null, null, Collections.emptyList(), null, null, sessionId).first();
        cohortId = cohort.getId();
        cohortUid = cohort.getUid();
        files.add(create("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"));
        files.add(create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz", true));
        files.add(create("platinum/1K.end.platinum-genomes-vcf-NA12879_S1.genome.vcf.gz"));
        files.add(create("platinum/1K.end.platinum-genomes-vcf-NA12880_S1.genome.vcf.gz", true));
        files.add(create("platinum/1K.end.platinum-genomes-vcf-NA12881_S1.genome.vcf.gz"));
        catalogManager.getCohortManager().setStatus(study.getFqn(), cohortName, "READY", "", sessionId);
    }

    @Before
    public void setUp() throws Exception {
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        Study study = catalogManager.getStudyManager().get(studyId, null, sessionId).first();

        StudyMetadata studyMetadata = metadataManager.createStudy(study.getFqn());
        for (File file : files) {
            metadataManager.registerFile(studyMetadata.getId(), file.getUri().getPath(), file.getSamples().stream()
                    .map(Sample::getId).collect(Collectors.toList()));
        }
        metadataManager.addIndexedFiles(studyMetadata.getId(), indexedFiles.stream()
                .map(f -> metadataManager.getFileId(studyMetadata.getId(), f))
                .collect(Collectors.toList()));
        studyConfigurationFactory = new CatalogStorageMetadataSynchronizer(catalogManager, metadataManager);
    }

    @After
    public void tearDown() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.writeAndClear(catalogManagerExternalResource.getOpencgaHome());
    }

    public static File create(String resourceName) throws IOException, CatalogException {
        return create(resourceName, false);
    }

    public static File create(String resourceName, boolean indexed) throws IOException, CatalogException {
        File file;
        URI uri = getResourceUri(resourceName);
        FileLinkParams params = new FileLinkParams()
                .setUri(uri.toString())
                .setPath("data/vcfs/");
        file = catalogManager.getFileManager().link(studyId, params, true, sessionId).first();
        if (indexed) {
            FileIndex fileIndex = new FileIndex("user", "today", new FileIndex.IndexStatus(FileIndex.IndexStatus.READY), 1234,
                    Collections.emptyMap());
            catalogManager.getFileManager().setFileIndex(studyId, file.getPath(), fileIndex, sessionId);
            indexedFiles.add(file.getName());
            List<String> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, sessionId).getResults().stream().map(Sample::getId).collect(Collectors.toList());
            samples.addAll(file.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            catalogManager.getCohortManager().update(studyId, cohortId,
                    new CohortUpdateParams().setSamples(samples), true, null, sessionId);
        }
        return catalogManager.getFileManager().get(studyId, file.getId(), null, sessionId).first();
    }

    @Test
    public void updateCatalogFromStudyConfigurationTest() throws Exception {

        StudyMetadata sc = studyConfigurationFactory.getStudyMetadata(studyId);

        List<String> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, sessionId)
                .getResults()
                .stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        samples.add(files.get(0).getSamples().get(0).getId());

        metadataManager.registerCohorts(studyId, Collections.singletonMap(cohortId, samples));

        catalogManager.getCohortManager().setStatus(studyId, cohortId, CohortStatus.CALCULATING, "", sessionId);

        File nonIndexedFile = files.stream().filter(file -> !indexedFiles.contains(file.getName())).findFirst().orElse(null);
        assertNotNull(nonIndexedFile);
        metadataManager.addIndexedFiles(sc.getId(), Collections.singletonList(metadataManager.getFileId(sc.getId(), nonIndexedFile.getName())));

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sc, sessionId);

        nonIndexedFile = catalogManager.getFileManager().get(studyId, nonIndexedFile.getName(), null, sessionId).first();
        assertEquals(FileIndex.IndexStatus.READY, nonIndexedFile.getInternal().getIndex().getStatus().getName());


        nonIndexedFile = files.stream().filter(file -> !indexedFiles.contains(file.getName())).findFirst().orElse(null);
        assertNotNull(nonIndexedFile);
        metadataManager.addRunningTask(sc.getId(), "LOAD", Collections.singletonList(metadataManager.getFileId(sc.getId(), nonIndexedFile.getName())), false, TaskMetadata.Type.LOAD);

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sc, sessionId);

        nonIndexedFile = catalogManager.getFileManager().get(studyId, nonIndexedFile.getName(), null, sessionId).first();
        assertEquals(FileIndex.IndexStatus.INDEXING, nonIndexedFile.getInternal().getIndex().getStatus().getName());

    }

    @Test
    public void testMissingSamples() throws CatalogException {
        String fileId = files.get(1).getId();
        catalogManager.getFileManager().update(studyId, fileId, new FileUpdateParams().setSamples(Collections.emptyList()), new QueryOptions(Constants.ACTIONS, Collections.singletonMap(FileDBAdaptor.QueryParams.SAMPLES.key(), "SET")), sessionId);

        catalogManager.getCohortManager().update(studyId, "ALL", new CohortUpdateParams().setSamples(Collections.singletonList("NA12878")), true, new QueryOptions(Constants.ACTIONS, Collections.singletonMap(FileDBAdaptor.QueryParams.SAMPLES.key(), "REMOVE")), sessionId);
        catalogManager.getSampleManager().delete(studyId, Collections.singletonList("NA12878"), new ObjectMap(), sessionId);

        assertEquals(0, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSamples().size());

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(studyId, sessionId);

        assertEquals(1, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSamples().size());
    }

    @Test
    public void testWrongSamples() throws CatalogException {

        String fileId = files.get(1).getId();
        String correctSampleId = files.get(1).getSamples().get(0).getId();
        String wrongSampleId = files.get(2).getSamples().get(0).getId();
        System.out.println("correctSampleId = " + correctSampleId);
        System.out.println("wrongSampleId = " + wrongSampleId);

        catalogManager.getFileManager().update(studyId, fileId, new FileUpdateParams().setSamples(Collections.singletonList(wrongSampleId)), new QueryOptions(Constants.ACTIONS, Collections.singletonMap(FileDBAdaptor.QueryParams.SAMPLES.key(), "SET")), sessionId);

        assertEquals(1, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSamples().size());
        assertEquals(wrongSampleId, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSamples().get(0).getId());

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(studyId, sessionId);

        assertEquals(1, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSamples().size());
        assertEquals(correctSampleId, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSamples().get(0).getId());
    }
}