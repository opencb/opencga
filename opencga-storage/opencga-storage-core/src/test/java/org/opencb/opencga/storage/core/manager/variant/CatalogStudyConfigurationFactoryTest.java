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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.manager.variant.metadata.CatalogStudyConfigurationFactory;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.dummy.DummyProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantFileMetadataDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created by hpccoll1 on 16/07/15.
 */
public class CatalogStudyConfigurationFactoryTest {
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
    static Logger logger = LoggerFactory.getLogger(CatalogStudyConfigurationFactoryTest.class);
    static private String catalogPropertiesFile;
    static private final String userId = "user";
    static private List<File> files = new ArrayList<>();
    static private LinkedHashSet<Integer> indexedFiles = new LinkedHashSet<>();
    private static String cohortId;
    private static long cohortUid;

    @BeforeClass
    public static void beforeClass() throws Exception {
//        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
//        stderr.setThreshold(Level.toLevel("debug"));

        catalogManager = catalogManagerExternalResource.getCatalogManager();
        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new FileUtils(catalogManager);

        User user = catalogManager.getUserManager().create(userId, "User", "user@email.org", "user", "ACME", null, Account.FULL, null, null).first();

        sessionId = catalogManager.getUserManager().login(userId, "user");
        projectId = catalogManager.getProjectManager().create("p1", "p1", "Project 1", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).first().getId();
        Study study = catalogManager.getStudyManager().create(projectId, "s1", null, "s1", Study.Type.CASE_CONTROL, null, "Study " +
                        "1", null, null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore("mongodb", DB_NAME)), null,
                null, null, sessionId).first();
        studyId = study.getId();
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

    @After
    public void tearDown() throws Exception {
        DummyProjectMetadataAdaptor.writeAndClear(catalogManagerExternalResource.getOpencgaHome());
        DummyStudyConfigurationAdaptor.writeAndClear(catalogManagerExternalResource.getOpencgaHome());
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
            indexedFiles.add((int) file.getUid());
            List<String> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, null, sessionId).getResult().stream().map(Sample::getId).collect(Collectors.toList());
            samples.addAll(file.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
            catalogManager.getCohortManager().update(studyId, cohortId, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), samples), true, null, sessionId);
        }
        return catalogManager.getFileManager().get(studyId, file.getId(), null, sessionId).first();
    }

    @Test
    public void getNewStudyConfiguration() throws Exception {
        CatalogStudyConfigurationFactory studyConfigurationFactory = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudyManager().get(studyId, null, sessionId).first();

        DummyStudyConfigurationAdaptor scAdaptor = spy(new DummyStudyConfigurationAdaptor());
        doReturn(new QueryResult<StudyConfiguration>("", 0, 0, 0, "", "", Collections.emptyList()))
                .when(scAdaptor).getStudyConfiguration(anyInt(), any(), any());
        StudyConfigurationManager scm = new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), scAdaptor, new DummyVariantFileMetadataDBAdaptor());
        StudyConfiguration studyConfiguration = studyConfigurationFactory.getStudyConfiguration(studyId, scm, new QueryOptions());

        checkStudyConfiguration(study, studyConfiguration);
    }

    @Test
    public void getNewStudyConfigurationNullManager() throws Exception {
        CatalogStudyConfigurationFactory studyConfigurationManager = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudyManager().get(studyId, null, sessionId).first();
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, null, new QueryOptions());

        checkStudyConfiguration(study, studyConfiguration);
    }

    @Test
    public void getStudyConfiguration() throws Exception {
        CatalogStudyConfigurationFactory studyConfigurationManager = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudyManager().get(studyId, null, sessionId).first();

        DummyStudyConfigurationAdaptor scAdaptor = spy(new DummyStudyConfigurationAdaptor());
        StudyConfiguration studyConfigurationToReturn = new StudyConfiguration((int) study.getUid(), "user@p1:s1");
        studyConfigurationToReturn.setIndexedFiles(indexedFiles);
        doReturn(new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(studyConfigurationToReturn)))
                .when(scAdaptor).getStudyConfiguration(anyInt(), any(), any());


        StudyConfigurationManager scm = new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), scAdaptor, new DummyVariantFileMetadataDBAdaptor());
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, scm, new QueryOptions());

        checkStudyConfiguration(study, studyConfiguration);
    }

    private void checkStudyConfiguration(Study study, StudyConfiguration studyConfiguration) throws CatalogException {
        assertEquals("user@p1:s1", studyConfiguration.getStudyName());
        assertEquals(study.getUid(), studyConfiguration.getStudyId());

        assertTrue(studyConfiguration.getInvalidStats().isEmpty());

        for (Map.Entry<String, Integer> entry : studyConfiguration.getFileIds().entrySet()) {
            File file = catalogManager.getFileManager().get(studyConfiguration.getStudyName(),
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
        CatalogStudyConfigurationFactory studyConfigurationFactory = new CatalogStudyConfigurationFactory(catalogManager);

        Study study = catalogManager.getStudyManager().get(studyId, null, sessionId).first();

        DummyStudyConfigurationAdaptor scAdaptor = spy(new DummyStudyConfigurationAdaptor());
        StudyConfiguration studyConfigurationToReturn = new StudyConfiguration((int) study.getUid(), "user@p1:s1");
        studyConfigurationToReturn.setIndexedFiles(indexedFiles);
        doReturn(new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(studyConfigurationToReturn)))
                .when(scAdaptor).getStudyConfiguration(anyInt(), any(), any());

        StudyConfigurationManager scm = new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), scAdaptor, new DummyVariantFileMetadataDBAdaptor());
        StudyConfiguration sc = studyConfigurationFactory.getStudyConfiguration(studyId, scm, new QueryOptions());

        List<Long> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, null, sessionId)
                .getResult()
                .stream()
                .map(Sample::getUid)
                .collect(Collectors.toList());
        samples.add(files.get(0).getSamples().get(0).getUid());

        sc.getCohorts().put(((int) cohortUid), samples.stream().map(Long::intValue).collect(Collectors.toSet()));

        catalogManager.getCohortManager().setStatus(studyId, cohortId, Cohort.CohortStatus.CALCULATING, "", sessionId);

        studyConfigurationFactory.updateCatalogFromStudyConfiguration(sc, sessionId);
    }
}