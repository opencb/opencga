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

package org.opencb.opencga.analysis.variant.metadata;

import org.junit.*;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.IndexStatus;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
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
import static org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer.secureGet;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created by hpccoll1 on 16/07/15.
 */
public class CatalogStorageMetadataSynchronizerTest {
    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    static private CatalogManager catalogManager;
    static private String sessionId;
    static private String projectId;
    static private String studyId;
    static Logger logger = LoggerFactory.getLogger(CatalogStorageMetadataSynchronizerTest.class);
    static private final String userId = "user";
    static private List<File> files = new ArrayList<>();
    static private LinkedHashSet<String> indexedFiles = new LinkedHashSet<>();
    private static String cohortId;
    private VariantStorageMetadataManager metadataManager;
    private CatalogStorageMetadataSynchronizer studyConfigurationFactory;

    @BeforeClass
    public static void beforeClass() throws Exception {
//        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
//        stderr.setThreshold(Level.toLevel("debug"));

        catalogManager = catalogManagerExternalResource.getCatalogManager();

        catalogManager.getUserManager().create(userId, "User", "user@email.org", TestParamConstants.PASSWORD, "ACME", null, Account.AccountType.FULL, null).first();

        sessionId = catalogManager.getUserManager().login(userId, TestParamConstants.PASSWORD).getToken();
        projectId = catalogManager.getProjectManager().create("p1", "p1", "Project 1", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionId).first().getId();
        Study study = catalogManager.getStudyManager().create(projectId, "s1", null, "s1", "Study " + "1", null, null,
                null, null, INCLUDE_RESULT, sessionId).first();
        studyId = study.getFqn();
        catalogManager.getFileManager().createFolder(studyId, Paths.get("data", "index").toString(),
                true, null, INCLUDE_RESULT, sessionId);
//        files.add(create("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
//        files.add(create("1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
//        files.add(create("1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
//        files.add(create("1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", true));
//        files.add(create("1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        String cohortName = "ALL";
        Cohort cohort = catalogManager.getCohortManager().create(studyId, new CohortCreateParams().setId(cohortName), null, null, INCLUDE_RESULT,
                sessionId).first();
        cohortId = cohort.getId();
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
            metadataManager.registerFile(studyMetadata.getId(), file.getUri().getPath(), file.getSampleIds());
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
            catalogManager.getFileManager().updateFileInternalVariantIndex(file,
                    FileInternalVariantIndex.init().setStatus(new VariantIndexStatus(InternalStatus.READY)), sessionId);
            indexedFiles.add(file.getName());
            List<String> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, sessionId).getResults().stream().map(Sample::getId).collect(Collectors.toList());
            samples.addAll(file.getSampleIds());
            List<SampleReferenceParam> sampleReferenceParams = samples.stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList());
            catalogManager.getCohortManager().update(studyId, cohortId,
                    new CohortUpdateParams().setSamples(sampleReferenceParams), true, null, sessionId);
        }
        return catalogManager.getFileManager().get(studyId, file.getId(), null, sessionId).first();
    }

    @Test
    public void updateCatalogFromStorageTest() throws Exception {

        StudyMetadata sm = studyConfigurationFactory.getStudyMetadata(studyId);

        List<String> samples = catalogManager.getCohortManager().getSamples(studyId, cohortId, sessionId)
                .getResults()
                .stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        samples.add(files.get(0).getSampleIds().get(0));

        metadataManager.registerCohorts(studyId, Collections.singletonMap(cohortId, samples));

        catalogManager.getCohortManager().setStatus(studyId, cohortId, CohortStatus.CALCULATING, "", sessionId);

        File nonIndexedFile = files.stream().filter(file -> !indexedFiles.contains(file.getName())).findFirst().orElse(null);
        assertNotNull(nonIndexedFile);
        metadataManager.addIndexedFiles(sm.getId(), Collections.singletonList(metadataManager.getFileId(sm.getId(), nonIndexedFile.getName())));

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        nonIndexedFile = catalogManager.getFileManager().get(studyId, nonIndexedFile.getName(), null, sessionId).first();
        assertEquals(VariantIndexStatus.READY, nonIndexedFile.getInternal().getVariant().getIndex().getStatus().getId());


        nonIndexedFile = files.stream().filter(file -> !indexedFiles.contains(file.getName())).findFirst().orElse(null);
        assertNotNull(nonIndexedFile);
        metadataManager.addRunningTask(sm.getId(), "LOAD", Collections.singletonList(metadataManager.getFileId(sm.getId(), nonIndexedFile.getName())), false, TaskMetadata.Type.LOAD);

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        nonIndexedFile = catalogManager.getFileManager().get(studyId, nonIndexedFile.getName(), null, sessionId).first();
        assertEquals(VariantIndexStatus.INDEXING, nonIndexedFile.getInternal().getVariant().getIndex().getStatus().getId());

    }

    @Test
    public void testInternalSampleStatuses() throws Exception {
        StudyMetadata sm = studyConfigurationFactory.getStudyMetadata(studyId);
        String fileName = indexedFiles.iterator().next();
        String sampleName = catalogManager.getFileManager().get(studyId, fileName, null, sessionId).first().getSampleIds().get(0);
        int version = sm.getSampleIndexConfigurationLatest().getVersion();
        Sample sample;

        //--------------
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);
        sample = catalogManager.getSampleManager().get(studyId, sampleName, null, sessionId).first();

        assertEquals(IndexStatus.READY, secureGet(sample, s -> s.getInternal().getVariant().getIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId(), null));

        //--------------
        metadataManager.updateSampleMetadata(sm.getId(), metadataManager.getSampleId(sm.getId(), sampleName), s -> s.setAnnotationStatus(TaskMetadata.Status.READY));
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        sample = catalogManager.getSampleManager().get(studyId, sampleName, null, sessionId).first();
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId(), null));

        //--------------
        metadataManager.updateSampleMetadata(sm.getId(), metadataManager.getSampleId(sm.getId(), sampleName), s -> s.setSampleIndexStatus(TaskMetadata.Status.READY, version));
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        sample = catalogManager.getSampleManager().get(studyId, sampleName, null, sessionId).first();
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId(), null));


        //--------------
        metadataManager.updateSampleMetadata(sm.getId(), metadataManager.getSampleId(sm.getId(), sampleName), s -> s.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, version));
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        sample = catalogManager.getSampleManager().get(studyId, sampleName, null, sessionId).first();
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId(), null));

        //--------------
        metadataManager.updateSampleMetadata(sm.getId(), metadataManager.getSampleId(sm.getId(), sampleName), s -> s.setFamilyIndexStatus(TaskMetadata.Status.READY, version));
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        sample = catalogManager.getSampleManager().get(studyId, sampleName, null, sessionId).first();
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId(), null));

        //--------------
        sm = metadataManager.updateStudyMetadata(sm.getId(), s -> {
            StudyMetadata.SampleIndexConfigurationVersioned c = s.getSampleIndexConfigurationLatest();
            c.setVersion(100);
            s.setSampleIndexConfigurations(Collections.singletonList(c));
            return s;
        });
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        sample = catalogManager.getSampleManager().get(studyId, sampleName, null, sessionId).first();
        assertEquals(IndexStatus.READY, secureGet(sample, s -> s.getInternal().getVariant().getIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.READY, secureGet(sample, s -> s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s -> s.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId(), null));
        assertEquals(IndexStatus.NONE, secureGet(sample, s->s.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId(), null));
        assertEquals(1, secureGet(sample, s -> s.getInternal().getVariant().getSecondarySampleIndex().getVersion(), null).intValue());
    }

    @Test
    public void testInternalFileStatuses() throws Exception {
        StudyMetadata sm = studyConfigurationFactory.getStudyMetadata(studyId);
        String fileName = indexedFiles.iterator().next();
        File file;

        // --------------------
        file = catalogManager.getFileManager().get(studyId, fileName, null, sessionId).first();

        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getIndex().getStatus().getId());
        assertEquals(IndexStatus.NONE, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
        assertEquals(IndexStatus.NONE, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());

        // --------------------
        metadataManager.updateFileMetadata(sm.getId(), metadataManager.getFileId(sm.getId(), fileName),
                fm -> fm.setAnnotationStatus(TaskMetadata.Status.READY));
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        file = catalogManager.getFileManager().get(studyId, fileName, null, sessionId).first();
        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getIndex().getStatus().getId());
        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
        assertEquals(IndexStatus.NONE, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());

        // --------------------
        metadataManager.updateFileMetadata(sm.getId(), metadataManager.getFileId(sm.getId(), fileName),
                fm -> fm.setSecondaryAnnotationIndexStatus(TaskMetadata.Status.READY));
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        file = catalogManager.getFileManager().get(studyId, fileName, null, sessionId).first();
        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getIndex().getStatus().getId());
        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());

        // --------------------
        metadataManager.updateFileMetadata(sm.getId(), metadataManager.getFileId(sm.getId(), fileName),
                fm -> fm.setAnnotationStatus(TaskMetadata.Status.NONE));
        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(sm, sessionId);

        file = catalogManager.getFileManager().get(studyId, fileName, null, sessionId).first();
        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getIndex().getStatus().getId());
        assertEquals(IndexStatus.NONE, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
        assertEquals(IndexStatus.READY, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
    }

    @Test
    public void testMissingSamples() throws CatalogException {
        String fileId = files.get(1).getId();
        catalogManager.getFileManager().update(studyId, fileId, new FileUpdateParams().setSampleIds(Collections.emptyList()), new QueryOptions(Constants.ACTIONS, Collections.singletonMap(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), "SET")), sessionId);

        catalogManager.getCohortManager().update(studyId, "ALL", new CohortUpdateParams()
                .setSamples(Collections.singletonList(new SampleReferenceParam().setId("NA12878"))), true, new QueryOptions(Constants.ACTIONS,
                Collections.singletonMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.REMOVE)), sessionId);
        catalogManager.getSampleManager().delete(studyId, Collections.singletonList("NA12878"), new QueryOptions(), sessionId);

        assertEquals(0, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSampleIds().size());

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(studyId, sessionId);

        assertEquals(1, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSampleIds().size());
    }

    @Test
    public void testWrongSamples() throws CatalogException {

        String fileId = files.get(1).getId();
        String correctSampleId = files.get(1).getSampleIds().get(0);
        String wrongSampleId = files.get(2).getSampleIds().get(0);
        System.out.println("correctSampleId = " + correctSampleId);
        System.out.println("wrongSampleId = " + wrongSampleId);

        catalogManager.getFileManager().update(studyId, fileId, new FileUpdateParams().setSampleIds(Collections.singletonList(wrongSampleId)), new QueryOptions(Constants.ACTIONS, Collections.singletonMap(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), "SET")), sessionId);

        assertEquals(1, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSampleIds().size());
        assertEquals(wrongSampleId, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSampleIds().get(0));

        studyConfigurationFactory.synchronizeCatalogStudyFromStorage(studyId, sessionId);

        assertEquals(1, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSampleIds().size());
        assertEquals(correctSampleId, catalogManager.getFileManager().get(studyId, fileId, new QueryOptions(), sessionId).first().getSampleIds().get(0));
    }
}