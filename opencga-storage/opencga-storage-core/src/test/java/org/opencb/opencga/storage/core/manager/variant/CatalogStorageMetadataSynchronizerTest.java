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

package org.opencb.opencga.storage.core.manager.variant;

import org.junit.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.models.update.CohortUpdateParams;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.manager.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;
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

        User user = catalogManager.getUserManager().create(userId, "User", "user@email.org", "user", "ACME", null, Account.Type.FULL, null, null).first();

        sessionId = catalogManager.getUserManager().login(userId, "user");
        projectId = catalogManager.getProjectManager().create("p1", "p1", "Project 1", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).first().getId();
        Study study = catalogManager.getStudyManager().create(projectId, "s1", null, "s1", Study.Type.CASE_CONTROL, null, "Study " +
                        "1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", DB_NAME)), null,
                null, null, sessionId).first();
        studyId = study.getFqn();
        studyUid = study.getUid();
        outputId = catalogManager.getFileManager().createFolder(studyId, Paths.get("data", "index").toString(), null,
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
//        checkStudyConfiguration(study, studyConfigurationToReturn);
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
        file = fileMetadataReader.create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
        catalogFileUtils.upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        if (indexed) {
            FileIndex fileIndex = new FileIndex("user", "today", new FileIndex.IndexStatus(FileIndex.IndexStatus.READY), 1234,
                    Collections.emptyMap());
            catalogManager.getFileManager().setFileIndex(studyId, file.getPath(), fileIndex, sessionId);
            indexedFiles.add(file.getName());
            List<String> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, sessionId).getResult().stream().map(Sample::getId).collect(Collectors.toList());
            samples.addAll(file.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            catalogManager.getCohortManager().update(studyId, cohortId, new CohortUpdateParams().setSamples(samples), true, null, sessionId);
        }
        return catalogManager.getFileManager().get(studyId, file.getId(), null, sessionId).first();
    }

    private void checkStudyConfiguration(Study study, StudyConfiguration studyConfiguration) throws CatalogException {
        assertEquals("user@p1:s1", studyConfiguration.getName());
        assertEquals(study.getUid(), studyConfiguration.getId());

        assertTrue(studyConfiguration.getInvalidStats().isEmpty());

        for (Map.Entry<String, Integer> entry : studyConfiguration.getFileIds().entrySet()) {
            File file = catalogManager.getFileManager().get(studyConfiguration.getName(),
                    studyConfiguration.getFileIds().inverse().get(entry.getValue()), null, sessionId).first();

            assertEquals(file.getName(), entry.getKey());
            int id = (int) file.getUid();
            assertEquals(file.getSamples().stream().map(Sample::getUid).map(Long::intValue).collect(Collectors.toSet()),
                    studyConfiguration.getSamplesInFiles().get((id)));
            if (file.getIndex() == null || file.getIndex().getStatus() == null || file.getIndex().getStatus().getName() == null
                    || !file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                assertFalse(studyConfiguration.getIndexedFiles().contains(id));
//                assertFalse("Should not contain header for file " + file.getId(), studyConfiguration.getHeaders().containsKey(id));
            }
//            else {
//                assertTrue(studyConfiguration.getIndexedFiles().contains(id));
//                assertTrue("Missing header for file " + file.getId(), studyConfiguration.getHeaders().containsKey(id));
//                assertTrue("Missing header for file " + file.getId(), !studyConfiguration.getHeaders().get(id).isEmpty());
//            }
        }
    }

    @Test
    public void updateCatalogFromStudyConfigurationTest() throws Exception {
        studyConfigurationFactory = new CatalogStorageMetadataSynchronizer(catalogManager, metadataManager);

        StudyMetadata sc = studyConfigurationFactory.getStudyMetadata(studyId);

        List<String> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, sessionId)
                .getResult()
                .stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        samples.add(files.get(0).getSamples().get(0).getId());

        metadataManager.registerCohorts(studyId, Collections.singletonMap(cohortId, samples));

        catalogManager.getCohortManager().setStatus(studyId, cohortId, Cohort.CohortStatus.CALCULATING, "", sessionId);

        File nonIndexedFile = files.stream().filter(file -> !indexedFiles.contains(file.getName())).findFirst().orElse(null);
        assertNotNull(nonIndexedFile);
        metadataManager.addIndexedFiles(sc.getId(), Collections.singletonList(metadataManager.getFileId(sc.getId(), nonIndexedFile.getName())));

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sc, sessionId);

        nonIndexedFile = catalogManager.getFileManager().get(studyId, nonIndexedFile.getName(), null, sessionId).first();
        assertEquals(FileIndex.IndexStatus.READY, nonIndexedFile.getIndex().getStatus().getName());


        nonIndexedFile = files.stream().filter(file -> !indexedFiles.contains(file.getName())).findFirst().orElse(null);
        assertNotNull(nonIndexedFile);
        metadataManager.addRunningTask(sc.getId(), "LOAD", Collections.singletonList(metadataManager.getFileId(sc.getId(), nonIndexedFile.getName())), false, TaskMetadata.Type.LOAD);

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sc, sessionId);

        nonIndexedFile = catalogManager.getFileManager().get(studyId, nonIndexedFile.getName(), null, sessionId).first();
        assertEquals(FileIndex.IndexStatus.INDEXING, nonIndexedFile.getIndex().getStatus().getName());

    }
}