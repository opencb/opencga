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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.junit.After;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.operations.VariantStatsIndexOperationTool;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.operations.variant.VariantStatsIndexParams;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyUpdateParams;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.catalog.db.api.CohortDBAdaptor.QueryParams.SAMPLES;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;
/**
 *
 * Created by hpccoll1 on 08/07/15.
 */
public class StatsVariantStorageTest extends AbstractVariantOperationManagerTest {

    Logger logger = LoggerFactory.getLogger(StatsVariantStorageTest.class);
    private String all;
    private String[] coh = new String[5];

    public void before () throws Exception {

        File file = opencga.createFile(studyId, "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz", sessionId);

        for (int i = 0; i < coh.length; i++) {
            List<SampleReferenceParam> sampleIds = file.getSampleIds().subList(file.getSampleIds().size() / coh.length * i,
                    file.getSampleIds().size() / coh.length * (i + 1))
                    .stream()
                    .map(s -> new SampleReferenceParam().setId(s))
                    .collect(Collectors.toList());
            Cohort cohort = catalogManager.getCohortManager().create(studyId, new CohortCreateParams("coh" + i,
                    "", Enums.CohortType.CONTROL_SET, "", null, null, sampleIds, null, null, null), null, null, new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), sessionId).first();
            coh[i] = cohort.getId();
        }
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false);
        variantManager.index(studyId, file.getId(), createTmpOutdir(file), queryOptions, sessionId);

        all = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(),
                DEFAULT_COHORT), new QueryOptions(), sessionId).first().getId();
    }

    public File beforeAggregated(String fileName, Aggregation aggregation) throws Exception {

        Map<String, Object> attributes;
        if (aggregation != null) {
            attributes = Collections.singletonMap(VariantStatsAnalysis.STATS_AGGREGATION_CATALOG, aggregation);
        } else {
            attributes = Collections.emptyMap();
        }
        StudyUpdateParams updateParams = new StudyUpdateParams()
                .setAttributes(attributes);
        catalogManager.getStudyManager().update(studyId, updateParams, null, sessionId);

        File file1 = opencga.createFile(studyId, fileName, sessionId);

//        coh0 = catalogManager.createCohort(studyId, "coh0", Cohort.Type.CONTROL_SET, "", file1.getSampleIds(), null, sessionId).first().getId();

        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false);
        variantManager.index(studyId, file1.getId(), createTmpOutdir(file1), queryOptions, sessionId);
        return file1;
    }

    public String createTmpOutdir(File file) throws CatalogException, IOException {
        return createTmpOutdir("_FILE_" + file.getUid());
    }

    public String createTmpOutdir(String sufix) throws CatalogException, IOException {
        return opencga.createTmpOutdir(studyId, sufix, sessionId);
    }


    public static List<Cohort> createCohorts(String sessionId, String studyId, String tagmapPath, CatalogManager catalogManager, Logger
            logger) throws IOException, CatalogException {
        List<Cohort> queryResults = new ArrayList<>();
        Properties tagmap = new Properties();
        tagmap.load(new FileInputStream(tagmapPath));
        Map<String, Cohort> cohorts = catalogManager.getCohortManager().search(studyId, new Query(), null, sessionId)
                .getResults().stream().collect(Collectors.toMap(Cohort::getId, c->c));
        Set<String> catalogCohorts = cohorts.keySet();
        for (String cohortName : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
            if (!catalogCohorts.contains(cohortName)) {
                DataResult<Cohort> cohort = catalogManager.getCohortManager().create(studyId, new Cohort()
                        .setId(cohortName)
                        .setSamples(Collections.emptyList())
                        .setType(Enums.CohortType.COLLECTION),
                        new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), sessionId);
                queryResults.add(cohort.first());
            } else {
                logger.warn("cohort {} was already created", cohortName);
                queryResults.add(cohorts.get(cohortName));
            }
        }
        return queryResults;
    }

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @After
    public void after () throws Exception {
//        catalogManagerTest.tearDown();
    }

    @Test
    public void testCalculateStatsOneByOne() throws Exception {
        before();
        
        Map<String, Cohort> cohorts = new HashMap<>();

        calculateStats(coh[0]);
        cohorts.put(coh[0], catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
//        cohorts.put("all", null);
        checkCalculatedStats(cohorts);

//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh1), sessionId, new QueryOptions(ExecutorFactory.EXECUTE, true)).first();
//        assertEquals(Status.READY, job.getStatus().getName());
        cohorts.put(coh[1], catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first());
        calculateStats(coh[1]);
        checkCalculatedStats(cohorts);

        calculateStats(coh[2]);
        cohorts.put(coh[2], catalogManager.getCohortManager().get(studyId, coh[2], null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(coh[3]);
        cohorts.put(coh[3], catalogManager.getCohortManager().get(studyId, coh[3], null, sessionId).first());
        checkCalculatedStats(cohorts);

        calculateStats(coh[4]);
        cohorts.put(coh[4], catalogManager.getCohortManager().get(studyId, coh[4], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    public void calculateStats(String cohortId) throws Exception {
        calculateStats(new VariantStatsIndexParams().setCohort(Arrays.asList(cohortId)));
    }

    public void calculateStats(VariantStatsIndexParams params) throws Exception {
        calculateStats(params, new QueryOptions());
    }

    public void calculateStats(VariantStatsIndexParams params, QueryOptions options) throws Exception {
        String tmpOutdir = createTmpOutdir("_STATS_" + String.join("_", Optional.ofNullable(params.getCohort()).orElse(Collections.emptyList())));
        options.put(ParamConstants.STUDY_PARAM, studyId);

        ToolRunner toolRunner = new ToolRunner(null, catalogManager, opencga.getStorageEngineFactory());
        toolRunner.execute(VariantStatsIndexOperationTool.class, params, options, Paths.get(tmpOutdir), null, sessionId);
    }

    @Test
    public void testCalculateStatsGroups() throws Exception {
        before();
        
        Map<String, Cohort> cohorts = new HashMap<>();

        calculateStats(new VariantStatsIndexParams().setCohort(Arrays.asList(coh[0], coh[1], coh[2])));
        cohorts.put(coh[0], catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
        cohorts.put(coh[1], catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first());
        cohorts.put(coh[2], catalogManager.getCohortManager().get(studyId, coh[2], null, sessionId).first());
        checkCalculatedStats(cohorts);

        try {
            calculateStats(new VariantStatsIndexParams().setCohort(Arrays.asList(all, coh[3], "-" + coh[4])));
            fail();
        } catch (ToolException e) {
            logger.info("received expected exception. this is OK, there is no cohort " + ("-" + coh[4]) + '\n');
        }
        assertEquals(CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, "ALL", null, sessionId).first().getInternal().getStatus().getId());
        assertEquals(CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, coh[3], null, sessionId).first().getInternal().getStatus().getId());

        assertEquals(CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, coh[4], null, sessionId).first().getInternal().getStatus().getId());

        calculateStats(new VariantStatsIndexParams().setCohort(Arrays.asList(all, coh[3], coh[4])));
        cohorts.put(DEFAULT_COHORT, catalogManager.getCohortManager().get(studyId, DEFAULT_COHORT, null, sessionId).first());
        cohorts.put(coh[3], catalogManager.getCohortManager().get(studyId, coh[3], null, sessionId).first());
        cohorts.put(coh[4], catalogManager.getCohortManager().get(studyId, coh[4], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateStats() throws Exception {
        before();


        assertEquals(CohortStatus.NONE, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getInternal().getStatus().getId());

        calculateStats(coh[0]);
        // TODO: Check status "CALCULATING"
//        Job job = variantStorage.calculateStats(outputId, Collections.singletonList(coh0), sessionId, new QueryOptions()).first();
//        assertEquals(Cohort.CohortStatus.CALCULATING, catalogManager.getCohort(coh0, null, sessionId).first().getStatus().getName());
//        runStorageJob(job, sessionId);
        assertEquals(CohortStatus.READY, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getInternal().getStatus().getId());

        Map<String, Cohort> cohorts = new HashMap<>();
        cohorts.put("coh0", catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
        checkCalculatedStats(cohorts);

        catalogManager.getCohortManager().update(studyId, coh[0],
                new CohortUpdateParams().setDescription("NewDescription"), new QueryOptions(), sessionId);
        assertEquals(CohortStatus.READY, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getInternal().getStatus().getId());

        List<SampleReferenceParam> newCohort = catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getSamples().stream()
                .map(s -> new SampleReferenceParam().setId(s.getId()))
                .skip(10).limit(100)
                .collect(Collectors.toList());
        catalogManager.getCohortManager().update(studyId, coh[0], new CohortUpdateParams().setSamples(newCohort),
                new QueryOptions(Constants.ACTIONS, Collections.singletonMap(SAMPLES.key(), ParamUtils.BasicUpdateAction.SET)), sessionId);
        assertEquals(CohortStatus.INVALID, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getInternal().getStatus().getId());

        calculateStats(coh[0]);
        assertEquals(CohortStatus.READY, catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first().getInternal().getStatus().getId());
        cohorts.put("coh0", catalogManager.getCohortManager().get(studyId, coh[0], null, sessionId).first());
        checkCalculatedStats(cohorts);
    }

    @Test
    public void testCalculateInvalidStats() throws Exception {
        before();

        calculateStats(coh[0]);

        DummyVariantStorageEngine vsm = mockVariantStorageManager();
        String message = "Error";
        doThrow(new StorageEngineException(message)).when(vsm).calculateStats(any(), any(List.class), any());
        doThrow(new StorageEngineException(message)).when(vsm).calculateStats(any(), any(Map.class), any());

        try {
            calculateStats(coh[1]);
            fail();
        } catch (ToolException e) {
            e.printStackTrace();
            assertEquals(message, e.getCause().getCause().getMessage());
        }

        Cohort coh1 = catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first();
        assertEquals(CohortStatus.INVALID, coh1.getInternal().getStatus().getId());

        vsm = mockVariantStorageManager();
        calculateStats(coh[1]);
    }

    @Test
    public void testResumeCalculateStats() throws Exception {
        before();

        calculateStats(coh[0]);

        catalogManager.getCohortManager().setStatus(studyId, coh[1], CohortStatus.CALCULATING, "", sessionId);
        Cohort coh1 = catalogManager.getCohortManager().get(studyId, coh[1], null, sessionId).first();
        Exception expected = VariantStatsOperationManager.unableToCalculateCohortCalculating(coh1);
        try {
            calculateStats(coh[1]);
            fail();
        } catch (ToolException e) {
            e.printStackTrace();
            Throwable actual = e.getCause();
            assertThat(actual, instanceOf(expected.getClass()));
            assertThat(actual, hasMessage(is(expected.getMessage())));
        }

        calculateStats(new VariantStatsIndexParams().setCohort(Collections.singletonList(coh[1])),
                new QueryOptions(VariantStorageOptions.RESUME.key(), true));

    }

    @Test
    public void testCalculateAggregatedStats() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", Aggregation.BASIC);

        calculateAggregatedStats(new VariantStatsIndexParams());
    }

    @Test
    public void testCalculateAggregatedStatsWithoutCohorts() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", Aggregation.BASIC);

        calculateStats(new VariantStatsIndexParams().setCohort(Arrays.asList()));
    }

    @Test
    public void testCalculateAggregatedStatsNonAggregatedStudy() throws Exception {
        beforeAggregated("variant-test-aggregated-file.vcf.gz", null);

        calculateAggregatedStats(new VariantStatsIndexParams().setAggregated(Aggregation.BASIC));

        Study study = catalogManager.getStudyManager().get(studyId, null, sessionId).first();

        String agg = study.getAttributes().get(VariantStatsAnalysis.STATS_AGGREGATION_CATALOG).toString();
        assertNotNull(agg);
        assertEquals(Aggregation.BASIC.toString(), agg);
    }

    public void calculateAggregatedStats(VariantStatsIndexParams params) throws Exception {
//        coh0 = catalogManager.createCohort(studyId, "ALL", Cohort.Type.COLLECTION, "", file.getSampleIds(), null, sessionId).first().getId();
        String cohId = catalogManager.getCohortManager().search(studyId, (Query) null, null, sessionId).first().getId();

        calculateStats(params.setCohort(Collections.singletonList(cohId)));

        checkCalculatedAggregatedStats(Collections.singleton(DEFAULT_COHORT), dbName);
    }

    @Test
    public void testCalculateAggregatedExacStats() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        URI uri = linkFile("exac-tag-mapping.properties").getUri();
        List<String> cohortIds = createCohorts(sessionId, studyId, uri.getPath(), catalogManager, logger)
                .stream().map(Cohort::getId).map(Object::toString).collect(Collectors.toList());

        calculateStats(new VariantStatsIndexParams().setCohort(cohortIds).setAggregationMappingFile("exac-tag-mapping.properties"));

        List<Cohort> cohorts = catalogManager.getCohortManager().search(studyId, (Query) null, null, sessionId).getResults();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(CohortStatus.READY, cohort.getInternal().getStatus().getId());
        }
//        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStatsExplicitCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        linkFile("exac-tag-mapping.properties");

        calculateStats(new VariantStatsIndexParams()
                .setCohort(Arrays.asList("AFR", "ALL", "AMR", "EAS", "FIN", "NFE", "OTH", "SAS"))
                .setAggregationMappingFile("exac-tag-mapping.properties"));

        List<Cohort> cohorts = catalogManager.getCohortManager().search(studyId, (Query) null, null, sessionId).getResults();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(CohortStatus.READY, cohort.getInternal().getStatus().getId());
        }
//        checkCalculatedAggregatedStats(cohorts, dbName);
    }

    @Test
    public void testCalculateAggregatedExacStatsWrongExplicitCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        linkFile("exac-tag-mapping.properties");

        thrown.expectCause(hasMessage(is(VariantStatsOperationManager.differentCohortsThanMappingFile().getMessage())));
        calculateStats(new VariantStatsIndexParams()
                .setCohort(Arrays.asList("AFR", "ALL"))
                .setAggregationMappingFile("exac-tag-mapping.properties"));
    }

    @Test
    public void testCalculateAggregatedExacMissingAggregationMappingFile() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        thrown.expect(hasCause(hasCause(hasMessage(is(VariantStatsAnalysis.missingAggregationMappingFile(Aggregation.EXAC).getMessage())))));
        calculateStats(new VariantStatsIndexParams());
    }

    @Test
    public void testCalculateNonAggregatedWithAggregationMappingFile() throws Exception {
        before();

        linkFile("exac-tag-mapping.properties");

        thrown.expect(ToolException.class);
        thrown.expectCause(hasCause(hasMessage(is(VariantStatsOperationManager.nonAggregatedWithMappingFile().getMessage()))));
        calculateStats(new VariantStatsIndexParams().setCohort(Arrays.asList("ALL")).setAggregationMappingFile("exac-tag-mapping.properties"));
    }

    @Test
    public void testCalculateAggregatedExacStatsWithoutCohorts() throws Exception {
        beforeAggregated("exachead.vcf.gz", Aggregation.EXAC);

        linkFile("exac-tag-mapping.properties");
        calculateStats(new VariantStatsIndexParams().setAggregationMappingFile("exac-tag-mapping.properties"));

        List<Cohort> cohorts = catalogManager.getCohortManager().search(studyId, (Query) null, null, sessionId).getResults();
        Set<String> cohortNames = cohorts
                .stream()
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        assertEquals(8, cohortNames.size());
        for (Cohort cohort : cohorts) {
            assertEquals(CohortStatus.READY, cohort.getInternal().getStatus().getId());
        }
//            checkCalculatedAggregatedStats(cohorts, dbName);

    }

    private File linkFile(String fileName) throws IOException, CatalogException {
        URI uri = getResourceUri(fileName);
        return catalogManager.getFileManager()
                .link(studyFqn, new FileLinkParams().setUri(uri.toString()), false, sessionId).first();
    }


    public void checkCalculatedStats(Map<String, Cohort> cohorts) throws Exception {
        checkCalculatedStats(studyId, cohorts, catalogManager, dbName, sessionId);
    }

    public static void checkCalculatedStats(String studyId, Map<String, Cohort> cohorts, CatalogManager catalogManager, String dbName, String sessionId) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageEngineFactory.get().getVariantStorageEngine(null, dbName).getDBAdaptor();

        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals("In variant " + variant.toString(), cohorts.size(), sourceEntry.getStats().size());
                for (VariantStats stats : sourceEntry.getStats()) {
                    assertTrue("In variant " + variant.toString(), cohorts.containsKey(stats.getCohortId()));
                    if (cohorts.get(stats.getCohortId()) != null) {
                        assertEquals("Variant: " + variant.toString() + " does not have the correct number of samples in cohort '" + stats.getCohortId() + "'. jsonVariant: " + variant.toJson() ,
                                cohorts.get(stats.getCohortId()).getSamples().size(),
                                stats.getGenotypeCount().values().stream().reduce(Integer::sum).orElse(0).intValue());
                    }
                }
            }
        }
        for (Cohort cohort : cohorts.values()) {
            cohort = catalogManager.getCohortManager().get(studyId, cohort.getId(), null, sessionId).first();
            assertEquals(CohortStatus.READY, cohort.getInternal().getStatus().getId());
        }
    }

    public static void checkCalculatedAggregatedStats(Set<String> cohortNames, String dbName) throws Exception {
        VariantDBAdaptor dbAdaptor = StorageEngineFactory.get().getVariantStorageEngine(null, dbName).getDBAdaptor();

        for (Variant variant : dbAdaptor) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(cohortNames, sourceEntry.getStats().stream().map(VariantStats::getCohortId).collect(Collectors.toSet()));
                for (VariantStats stats : sourceEntry.getStats()) {
                    assertTrue(cohortNames.contains(stats.getCohortId()));
                }
            }
        }
    }

}