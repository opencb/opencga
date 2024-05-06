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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.operations.VariantFileIndexJobLauncherTool;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.variant.VariantFileIndexJobLauncherParams;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.core.api.ParamConstants.STUDY_PARAM;

/**
 * Created on 15/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class PlatinumFileIndexerTest extends AbstractVariantOperationManagerTest {

    private Logger logger = LoggerFactory.getLogger(AbstractVariantOperationManagerTest.class);

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @Before
    public void before() throws CatalogException {
    }

    @Test
    public void testBySteps() throws Exception {
        File inputFile;
        File transformFile;
        for (int i = 77; i <= 93; i++) {
            inputFile = create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
            transformFile = transformFile(inputFile, new QueryOptions());
            loadFile(transformFile, new QueryOptions(), outputId);
        }


        variantManager.iterator(new Query(VariantQueryParam.STUDY.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    @Test
    public void testIndexFamily() throws Exception {
        List<File> inputFiles = new LinkedList<>();
        for (int i = 77; i <= 93; i++) {
            inputFiles.add(create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz"));
        }

        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantManager.getStorageConfiguration()));

        Path outDir = Paths.get(opencga.createTmpOutdir("_indexFamily"));
        System.out.println("outDir = " + outDir);
        VariantIndexParams params = new VariantIndexParams();
        params.setFile(inputFiles.stream().map(File::getName).collect(Collectors.joining(",")));
        params.setFamily(true);

        ExecutionResult er = toolRunner.execute(VariantIndexOperationTool.class, params.toObjectMap()
                .append(ParamConstants.STUDY_PARAM, studyId), outDir, null, sessionId);

        assertEquals(2, er.getSteps().size());
        assertEquals("variant-index", er.getSteps().get(0).getId());
        assertEquals("family-index", er.getSteps().get(1).getId());

        variantManager.iterator(new Query(VariantQueryParam.STUDY.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    @Test
    public void testByStepsMultiRelease() throws Exception {

        List<File> inputFiles = new ArrayList<>();
        File transformFile;

        for (int i = 77; i <= 79; i++) {
            inputFiles.add(create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz"));
        }

        for (File inputFile : inputFiles) {
            transformFile = transformFile(inputFile, new QueryOptions());
            loadFile(transformFile, new QueryOptions(), outputId);

            opencga.getCatalogManager().getProjectManager().incrementRelease(projectId, sessionId);
        }

        int i = 1;
        for (File inputFile : inputFiles) {
            inputFile = opencga.getCatalogManager().getFileManager().get(studyId, inputFile.getId(), null, sessionId).first();
            assertEquals(1, inputFile.getRelease());
            assertEquals(i, inputFile.getInternal().getVariant().getIndex().getRelease());
            i++;
        }

        variantManager.iterator(new Query(VariantQueryParam.STUDY.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    @Test
    public void testBatch() throws Exception {

        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            files.add(create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz"));
        }
        indexFiles(files, new QueryOptions(), outputId);

        variantManager.iterator(new Query(VariantQueryParam.STUDY.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    @Test
    public void testBatchFromDirectory() throws Exception {

        List<String> names = new ArrayList<>(17);
        List<File> files = new ArrayList<>(17);
        for (int i = 77; i <= 93; i++) {
            names.add("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
        }
        // Use same seed
        Collections.shuffle(names, new Random(0));
        for (String name : names) {
            System.out.println(name);
            files.add(create(name));
        }

        File directory = catalogManager.getFileManager().get("" + studyId, "data/vcfs/", null, sessionId).first();
        List<StoragePipelineResult> results = indexFiles(Collections.singletonList(directory), files, new QueryOptions(), outputId);

        List<String> fileNames = results.stream().map(StoragePipelineResult::getInput).map(URI::toString).collect(Collectors.toList());
        assertTrue(ArrayUtils.isSorted(fileNames.toArray(new String[]{})));

        variantManager.iterator(new Query(VariantQueryParam.STUDY.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    @Test
    public void testBatchBySteps() throws Exception {

        File inputFile;
        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            inputFile = create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
            files.add(transformFile(inputFile, new QueryOptions()));
        }
        loadFiles(files, new QueryOptions(), outputId);

        variantManager.iterator(new Query(VariantQueryParam.STUDY.key(), studyId), new QueryOptions(), sessionId).forEachRemaining(variant -> {
            System.out.println("variant = " + variant);
        });
    }

    @Test
    public void testLauncher() throws CatalogException, IOException, ToolException {
        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantManager.getStorageConfiguration()));
        int numFiles = 0;
        for (int i = 77; i <= 93; i++) {
            create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz");
            numFiles++;
        }
        VariantFileIndexJobLauncherParams params = new VariantFileIndexJobLauncherParams().setDirectory("data/vcfs");
        List<String> tags = Arrays.asList("tag1", "tag2");
        Job job = catalogManager.getJobManager().submit(studyFqn, VariantFileIndexJobLauncherTool.ID, Enums.Priority.HIGH,
                params.toParams(STUDY_PARAM, studyFqn), null, null, null, tags, null, null, sessionId).first();
        ExecutionResult result = toolRunner.execute(job, Paths.get(opencga.createTmpOutdir(studyId, "_LOAD_", sessionId)), sessionId);

        List<String> tagsFromResult = result.getAttributes().getAsStringList(VariantFileIndexJobLauncherTool.JOB_TAGS_ATTRIBUTE);
        assertTrue(tagsFromResult.containsAll(tags));
        for (String tag : tagsFromResult) {
            List<Job> results = catalogManager.getJobManager().search(studyFqn,
                    new Query()
                            .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), VariantIndexOperationTool.ID)
                            .append(JobDBAdaptor.QueryParams.TAGS.key(), tag),
                    new QueryOptions(), sessionId).getResults();
            assertEquals(numFiles, results.size());
        }
        assertEquals(numFiles, result.getAttributes().getInt(VariantFileIndexJobLauncherTool.SUBMITTED_JOBS_ATTRIBUTE));
        assertEquals(numFiles, result.getAttributes().getInt(VariantFileIndexJobLauncherTool.SCANNED_FILES_ATTRIBUTE));

        //// Execute again, no new jobs should be submitted
        tags = Arrays.asList("tag10", "tag20");
        job = catalogManager.getJobManager().submit(studyFqn, VariantFileIndexJobLauncherTool.ID, Enums.Priority.HIGH,
                params.toParams(STUDY_PARAM, studyFqn), null, null, null, tags, null, null, sessionId).first();
        result = toolRunner.execute(job, Paths.get(opencga.createTmpOutdir(studyId, "_LOAD_", sessionId)), sessionId);

        tagsFromResult = result.getAttributes().getAsStringList(VariantFileIndexJobLauncherTool.JOB_TAGS_ATTRIBUTE);
        assertTrue(tagsFromResult.containsAll(tags));
        for (String tag : tagsFromResult) {
            List<Job> results = catalogManager.getJobManager().search(studyFqn,
                    new Query()
                            .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), VariantIndexOperationTool.ID)
                            .append(JobDBAdaptor.QueryParams.TAGS.key(), tag),
                    new QueryOptions(), sessionId).getResults();
            assertEquals(0, results.size());
        }
        assertEquals(0, result.getAttributes().getInt(VariantFileIndexJobLauncherTool.SUBMITTED_JOBS_ATTRIBUTE));
        assertEquals(numFiles, result.getAttributes().getInt(VariantFileIndexJobLauncherTool.SCANNED_FILES_ATTRIBUTE));
    }

}
