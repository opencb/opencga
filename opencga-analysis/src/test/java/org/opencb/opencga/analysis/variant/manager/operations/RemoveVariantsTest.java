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

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created on 10/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class RemoveVariantsTest extends AbstractVariantOperationManagerTest {

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testLoadAndRemoveOne() throws Exception {

        File file77 = create("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        indexFile(file77, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);

        removeFile(file77, new QueryOptions());

        // File already transformed. Just load
        loadFile(file77, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);

    }

    @Test
    public void testLoadAndRemoveOneWithOtherLoaded() throws Exception {
        File file78 = create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        indexFile(file78, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);

        testLoadAndRemoveOne();
    }

    @Test
    public void testLoadAndRemoveForce() throws Exception {
        File file77 = create("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        File file78 = create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        File file79 = create("platinum/1K.end.platinum-genomes-vcf-NA12879_S1.genome.vcf.gz");
        indexFile(file77, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);
        indexFile(file78, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);

        removeFile(file77, new QueryOptions());

        try {
            removeFile(file77, new QueryOptions());
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Unable to remove variants from file"));
        }
        removeFile(file77, new QueryOptions(VariantStorageOptions.FORCE.key(), true));

        try {
            removeFile(file79, new QueryOptions(VariantStorageOptions.FORCE.key(), true));
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("File not found in storage."));
        }
        Path file77Path = Paths.get(file77.getUri());
        Path otherDir = file77Path.getParent().resolve("other_dir");
        Files.createDirectory(otherDir);
        Path otherFile = Files.copy(file77Path, otherDir.resolve(file77Path.getFileName()));
        File file77_2 = create(studyFqn, otherFile.toUri(), "other_dir");

        try {
            removeFile(studyFqn, Collections.singletonList(file77_2.getPath()), new QueryOptions(VariantStorageOptions.FORCE.key(), true));
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Unable to remove variants from file"));
            assertTrue(e.getMessage(), e.getMessage().contains("Instead, found file with same name but different path"));
        }
    }


    @Test
    public void testLoadAndRemoveMany() throws Exception {
        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            files.add(create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz"));
        }
        indexFiles(files, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);

        removeFile(files.subList(0, files.size() / 2), new QueryOptions());

    }

    @Test
    public void testLoadAndRemoveDuplicated() throws Exception {
        List<File> files = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            File file = create(studyId, getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz", "vcfs" + i + "/file.vcf.gz"), "data/vcfs" + i + "/");
            String outputId = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data", "index_" + i).toString(), true, null,
                    QueryOptions.empty(), sessionId).first().getId();
            files.add(file);

            indexFiles(Arrays.asList(file), new QueryOptions(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true), outputId);
        }

        removeFile(files.subList(0, files.size() / 2), new QueryOptions());
    }

    @Test
    public void testLoadAndRemoveDifferentChromosomes() throws Exception {
        List<File> files = new ArrayList<>();
        files.add(create("by_chr/chr20.variant-test-file.vcf.gz"));
        files.add(create("by_chr/chr21.variant-test-file.vcf.gz"));
        indexFiles(files, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.CHROMOSOME), outputId);

        removeFile(files.get(0), new QueryOptions());
    }

    @Test
    public void testLoadAndRemoveStudy() throws Exception {
        File file77 = create("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        File file78 = create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");

        indexFile(file77, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);
        indexFile(file78, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);

        removeStudy(studyId, new QueryOptions(), 2, 2);
    }

    @Test
    public void testLoadAndRemoveSample() throws Exception {
        File file77 = create("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        File file78 = create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");

        indexFile(file77, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);
        indexFile(file78, new QueryOptions(VariantStorageOptions.ANNOTATE.key(), true), outputId);

        removeSample(studyId, Arrays.asList("NA12878"), new QueryOptions());
    }

    private void removeFile(File file, QueryOptions options) throws Exception {
        removeFile(Collections.singletonList(file), options);
    }

    private void removeFile(List<File> files, QueryOptions options) throws Exception {
        List<String> fileIds = files.stream().map(File::getId).collect(Collectors.toList());

        Study study = catalogManager.getFileManager().getStudy(ORGANIZATION, files.get(0), sessionId);
        String studyId = study.getFqn();

        removeFile(studyId, fileIds, options);
    }

    private void removeFile(String studyId, List<String> fileIds, QueryOptions options) throws Exception {

        for (String fileId : fileIds) {
            File file = catalogManager.getFileManager().get(studyId, fileId, null, sessionId).first();
            if (file.getInternal().getVariant().getIndex().getStatus().getId().equals(VariantIndexStatus.READY)) {
                assertEquals(VariantIndexStatus.READY, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                assertEquals(VariantIndexStatus.NONE, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                for (String sampleId : file.getSampleIds()) {
                    Sample sample = catalogManager.getSampleManager().get(studyId, sampleId, null, sessionId).first();
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId());
                }
            }
        }

        Path outdir = Paths.get(opencga.createTmpOutdir(studyId, "_REMOVE_", sessionId));
        variantManager.removeFile(studyId, fileIds, new QueryOptions(options), outdir.toUri(), sessionId);
//        assertEquals(files.size(), removedFiles.size());

        Cohort all = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(),
                StudyEntry.DEFAULT_COHORT), null, sessionId).first();
        Set<String> allSampleIds = all.getSamples().stream().map(Sample::getId).collect(Collectors.toSet());

        assertThat(all.getInternal().getStatus().getId(), anyOf(is(CohortStatus.INVALID), is(CohortStatus.NONE)));
        Set<String> loadedSamples = catalogManager.getFileManager().search(studyId, new Query(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key
                        (), VariantIndexStatus.READY), null, sessionId)
                .getResults()
                .stream()
                .flatMap(f -> f.getSampleIds().stream())
                .collect(Collectors.toSet());
        assertEquals(loadedSamples, allSampleIds);

        for (String fileId : fileIds) {
            File file = catalogManager.getFileManager().get(studyId, fileId, null, sessionId).first();
            assertEquals(VariantIndexStatus.TRANSFORMED, file.getInternal().getVariant().getIndex().getStatus().getId());
            assertEquals(VariantIndexStatus.NONE, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
            assertEquals(VariantIndexStatus.NONE, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
            for (String sampleId : file.getSampleIds()) {
                Sample sample = catalogManager.getSampleManager().get(studyId, sampleId, null, sessionId).first();
                if (sample.getFileIds().size() == 1) {
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId());
                } else {
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId());
                }
            }
        }

    }

    private void removeSample(String studyId, List<String> sampleIds, QueryOptions options) throws Exception {

        Set<String> fileIds = new HashSet<>();
        catalogManager.getSampleManager().get(studyId, sampleIds, null, sessionId).getResults().forEach(
                sample -> fileIds.addAll(sample.getFileIds())
        );

        for (String fileId : fileIds) {
            File file = catalogManager.getFileManager().get(studyId, fileId, null, sessionId).first();
            if (file.getInternal().getVariant().getIndex().getStatus().getId().equals(VariantIndexStatus.READY)) {
                assertEquals(VariantIndexStatus.READY, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                assertEquals(VariantIndexStatus.NONE, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                for (String sampleId : file.getSampleIds()) {
                    Sample sample = catalogManager.getSampleManager().get(studyId, sampleId, null, sessionId).first();
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId());
                }
            }
        }

        Path outdir = Paths.get(opencga.createTmpOutdir(studyId, "_REMOVE_", sessionId));
        variantManager.removeSample(studyId, sampleIds, new QueryOptions(options), outdir.toUri(), sessionId);
//        assertEquals(files.size(), removedFiles.size());

        Cohort all = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(),
                StudyEntry.DEFAULT_COHORT), null, sessionId).first();
        Set<String> allSampleIds = all.getSamples().stream().map(Sample::getId).collect(Collectors.toSet());

        assertThat(all.getInternal().getStatus().getId(), anyOf(is(CohortStatus.INVALID), is(CohortStatus.NONE)));
        Set<String> loadedSamples = catalogManager.getFileManager().search(studyId, new Query(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key
                        (), VariantIndexStatus.READY), null, sessionId)
                .getResults()
                .stream()
                .flatMap(f -> f.getSampleIds().stream())
                .collect(Collectors.toSet());
        assertEquals(loadedSamples, allSampleIds);

        for (String fileId : fileIds) {
            File file = catalogManager.getFileManager().get(studyId, fileId, null, sessionId).first();
            assertEquals(VariantIndexStatus.TRANSFORMED, file.getInternal().getVariant().getIndex().getStatus().getId());
            assertEquals(VariantIndexStatus.NONE, file.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
            assertEquals(VariantIndexStatus.NONE, file.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
            for (String sampleId : file.getSampleIds()) {
                Sample sample = catalogManager.getSampleManager().get(studyId, sampleId, null, sessionId).first();
                if (sample.getFileIds().size() == 1) {
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId());
                } else {
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.NONE, sample.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId());
                    assertEquals(VariantIndexStatus.READY, sample.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId());
                }
            }
        }

    }

    private void removeStudy(Object study, QueryOptions options, int expectedNumFiles, int expectedNumSamples) throws Exception {
        Query query = new Query(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(expectedNumFiles, catalogManager.getFileManager().count(study.toString(), query, sessionId).getNumTotalResults());
        query = new Query(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(expectedNumFiles, catalogManager.getFileManager().count(study.toString(), query, sessionId).getNumTotalResults());

        query = new Query(SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(expectedNumSamples, catalogManager.getSampleManager().count(study.toString(), query, sessionId).getNumTotalResults());
        query = new Query(SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(expectedNumSamples, catalogManager.getSampleManager().count(study.toString(), query, sessionId).getNumTotalResults());
        query = new Query(SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_SECONDARY_SAMPLE_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(expectedNumSamples, catalogManager.getSampleManager().count(study.toString(), query, sessionId).getNumTotalResults());


        Path outdir = Paths.get(opencga.createTmpOutdir(studyId, "_REMOVE_", sessionId));
        variantManager.removeStudy(study.toString(), options, outdir.toUri(), sessionId);

        query = new Query(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(0L, catalogManager.getFileManager().count(study.toString(), query, sessionId).getNumTotalResults());
        query = new Query(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(0L, catalogManager.getFileManager().count(study.toString(), query, sessionId).getNumTotalResults());

        Cohort all = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT), null, sessionId).first();
        assertTrue(all.getSamples().isEmpty());

        query = new Query(SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(0L, catalogManager.getSampleManager().count(study.toString(), query, sessionId).getNumTotalResults());
        query = new Query(SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(0L, catalogManager.getSampleManager().count(study.toString(), query, sessionId).getNumTotalResults());
        query = new Query(SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_SECONDARY_SAMPLE_INDEX_STATUS_ID.key(), VariantIndexStatus.READY);
        assertEquals(0L, catalogManager.getSampleManager().count(study.toString(), query, sessionId).getNumTotalResults());


    }


}
