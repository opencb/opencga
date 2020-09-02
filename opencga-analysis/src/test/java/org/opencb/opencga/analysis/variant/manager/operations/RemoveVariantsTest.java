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
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.ResourceReference;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileIndex;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Created on 10/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
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
        indexFile(file77, new QueryOptions(), outputId);

        removeFile(file77, new QueryOptions());

        // File already transformed. Just load
        loadFile(file77, new QueryOptions(), outputId);

    }

    @Test
    public void testLoadAndRemoveOneWithOtherLoaded() throws Exception {
        File file78 = create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        indexFile(file78, new QueryOptions(), outputId);

        testLoadAndRemoveOne();
    }


    @Test
    public void testLoadAndRemoveMany() throws Exception {
        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            files.add(create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz"));
        }
        indexFiles(files, new QueryOptions(), outputId);

        removeFile(files.subList(0, files.size()/2), new QueryOptions());

    }

    @Test
    public void testLoadAndRemoveDifferentChromosomes() throws Exception {
        List<File> files = new ArrayList<>();
        files.add(create("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        files.add(create("10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));
        indexFiles(files, new QueryOptions(), outputId);

        removeFile(files.get(0), new QueryOptions());
    }

    @Test
    public void testLoadAndRemoveStudy() throws Exception {
        File file77 = create("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        File file78 = create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");

        indexFile(file77, new QueryOptions(), outputId);
        indexFile(file78, new QueryOptions(), outputId);

        removeStudy(studyId, new QueryOptions());
    }

    private void removeFile(File file, QueryOptions options) throws Exception {
        removeFile(Collections.singletonList(file), options);
    }

    private void removeFile(List<File> files, QueryOptions options) throws Exception {
        List<String> fileIds = files.stream().map(File::getId).collect(Collectors.toList());

        Study study = catalogManager.getFileManager().getStudy(files.get(0), sessionId);
        String studyId = study.getFqn();

        Path outdir = Paths.get(opencga.createTmpOutdir(studyId, "_REMOVE_", sessionId));
        variantManager.removeFile(studyId, fileIds, new QueryOptions(), sessionId);
//        assertEquals(files.size(), removedFiles.size());

        Cohort all = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(),
                StudyEntry.DEFAULT_COHORT), null, sessionId).first();
        Set<Long> allSampleIds = all.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet());

        assertThat(all.getInternal().getStatus().getName(), anyOf(is(CohortStatus.INVALID), is(CohortStatus.NONE)));
        Set<Long> loadedSamples = catalogManager.getFileManager().search(studyId, new Query(FileDBAdaptor.QueryParams.INTERNAL_INDEX_STATUS_NAME.key
                (), FileIndex.IndexStatus.READY), null, sessionId)
                .getResults()
                .stream()
                .flatMap(f -> f.getSamples().stream())
                .map(ResourceReference::getUid)
                .collect(Collectors.toSet());
        assertEquals(loadedSamples, allSampleIds);

        for (String file : fileIds) {
            assertEquals(FileIndex.IndexStatus.TRANSFORMED, catalogManager.getFileManager().get(studyId, file, null, sessionId).first().getInternal().getIndex().getStatus().getName());
        }

    }

    private void removeStudy(Object study, QueryOptions options) throws Exception {
        variantManager.removeStudy(study.toString(), options, sessionId);

        Query query = new Query(FileDBAdaptor.QueryParams.INTERNAL_INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY);
        assertEquals(0L, catalogManager.getFileManager().count(study.toString(), query, sessionId).getNumTotalResults());

        Cohort all = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT), null, sessionId).first();
        assertTrue(all.getSamples().isEmpty());
    }


}
