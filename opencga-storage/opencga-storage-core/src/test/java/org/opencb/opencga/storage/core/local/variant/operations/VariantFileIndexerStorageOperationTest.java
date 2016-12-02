/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local.variant.operations;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.variant.AbstractVariantStorageOperationTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageETL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.local.variant.operations.StatsVariantStorageTest.checkCalculatedStats;

/**
 * Created by hpccoll1 on 13/07/15.
 */
public class VariantFileIndexerStorageOperationTest extends AbstractVariantStorageOperationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Logger logger = LoggerFactory.getLogger(VariantFileIndexerStorageOperationTest.class);
    private List<File> files;
    private final static String[] FILE_NAMES = {
            "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"};
    @Before
    public void beforeIndex() throws Exception {
        files = Arrays.asList(new File[5]);
    }

    @Test
    public void testIndexWithStats() throws Exception {

        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false);
        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), false);

        variantManager.index(String.valueOf(getFile(0).getId()), newTmpOutdir(),
                String.valueOf(outputId), queryOptions, sessionId);
        assertEquals(500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(getFile(0).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        variantManager.index(String.valueOf(getFile(1).getId()), newTmpOutdir(),
                String.valueOf(outputId), queryOptions, sessionId);
        assertEquals(1000, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.NONE, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(getFile(1).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        variantManager.index(String.valueOf(getFile(2).getId()), newTmpOutdir(),
                String.valueOf(outputId), queryOptions, sessionId);
        assertEquals(1500, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort(studyId).getStatus().getName());
        checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, catalogManager.getAllCohorts(studyId,
                new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()),
                catalogManager, dbName, sessionId);
        assertNotNull(catalogManager.getFile(getFile(2).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), false);
        variantManager.index(String.valueOf(getFile(3).getId()), newTmpOutdir(),
                String.valueOf(outputId), queryOptions, sessionId);
        assertEquals(2000, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.INVALID, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(getFile(3).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        variantManager.index(String.valueOf(getFile(4).getId()), newTmpOutdir(),
                String.valueOf(outputId), queryOptions, sessionId);
        assertEquals(2504, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort(studyId).getStatus().getName());
        assertNotNull(catalogManager.getFile(getFile(4).getId(), sessionId).first().getStats().get(FileMetadataReader.VARIANT_STATS));
        checkCalculatedStats(Collections.singletonMap(DEFAULT_COHORT, catalogManager.getAllCohorts(studyId,
                new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT), new QueryOptions(), sessionId).first()),
                catalogManager, dbName, sessionId);
    }

    protected File getFile(int index) throws IOException, CatalogException {
        if (files.get(index) == null) {
            files.set(index, create(FILE_NAMES[index]));
        }
        return files.get(index);
    }

    String newTmpOutdir() throws CatalogException {
        return opencga.createTmpOutdir(studyId, "index", sessionId);
    }

    @Test
    public void testIndex() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false);

        indexFile(getFile(0), queryOptions, outputId);
    }

    @Test
    public void testIndexFromFolder() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false);
        File file = getFile(0);
        File parent = catalogManager.getFileParent(file.getId(), null, sessionId).first();
        indexFiles(singletonList(parent), singletonList(file), queryOptions, outputId);
    }

    @Test
    public void testIndexBySteps() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false);

        File transformedFile = transformFile(getFile(0), queryOptions);
        loadFile(transformedFile, queryOptions, outputId);
    }

    @Test
    public void testIndexByStepsWithStats() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);

        File transformedFile = transformFile(getFile(0), queryOptions);
        loadFile(transformedFile, queryOptions, outputId);
    }

    @Test
    public void testIndexByStepsSameInput() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);

        transformFile(getFile(0), queryOptions);
        loadFile(getFile(0), queryOptions, outputId);

    }

    @Test
    public void testIndexWithError() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false);

        DummyVariantStorageETL storageETL = mockVariantStorageETL();
        List<File> files = Arrays.asList(getFile(0), getFile(1));
        StorageManagerException loadException = StorageManagerException.unableToExecute("load", 0, "");
        Mockito.doThrow(loadException).when(storageETL)
                .load(ArgumentMatchers.argThat(argument -> argument.toString().contains(files.get(1).getName())));

        try {
            indexFiles(files, queryOptions, outputId);
        } catch (StorageETLException exception) {
            assertEquals(files.size(), exception.getResults().size());

            for (int i = files.size(); i > 0; i--) {
                assertTrue(exception.getResults().get(1).isTransformExecuted());
                assertNull(exception.getResults().get(1).getTransformError());
            }

            assertTrue(exception.getResults().get(0).isLoadExecuted());
            assertNull(exception.getResults().get(0).getLoadError());

            assertTrue(exception.getResults().get(1).isLoadExecuted());
            assertSame(loadException, exception.getResults().get(1).getLoadError());
        }

        mockVariantStorageETL();
        // File 0 already loaded.
        // Expecting to load only file 1
        loadFiles(files, singletonList(files.get(1)), queryOptions, outputId);
    }

    @Test
    public void testIndexByStepsExternallyTransformed() throws Exception {
        QueryOptions queryOptions = new QueryOptions()
                // TODO: Should work without isolating transformation?
                .append(VariantStorageManager.Options.ISOLATE_FILE_FROM_STUDY_CONFIGURATION.key(), true);

        File transformFile = transformFile(getFile(0), queryOptions);
        Query searchQuery = new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), transformFile.getJob().getId())
                .append(FileDBAdaptor.QueryParams.NAME.key(), "~file.(json|avro)");
        File transformSourceFile = catalogManager.getAllFiles(studyId, searchQuery, new QueryOptions(), sessionId).first();

        create(studyId2, catalogManager.getFileUri(getFile(0)));
        create(studyId2, catalogManager.getFileUri(transformSourceFile));
        transformFile = create(studyId2, catalogManager.getFileUri(transformFile));
        catalogManager.getFileManager().matchUpVariantFiles(singletonList(transformFile), sessionId);

        queryOptions = new QueryOptions().append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true);
        loadFile(transformFile, queryOptions, outputId2);

    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }
}